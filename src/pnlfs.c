#include <linux/buffer_head.h>
#include <linux/fs.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/string.h>

#include "pnlfs.h"

#define PNLFS_WORD_SIZE sizeof(long)
#define PNLFS_NR_WORDS_IN_BLOCK (PNLFS_BLOCK_SIZE / PNLFS_WORD_SIZE)
#define PNLFS_NR_INODES_IN_BLOCK (PNLFS_BLOCK_SIZE / \
		sizeof(struct pnlfs_inode))

MODULE_DESCRIPTION("pnlfs");
MODULE_AUTHOR("Killian Valverde");
MODULE_LICENSE("GPL");

//------------------------------------------------------------------------------
/**
 * Get a &pnlfs_inode_info from the underlying inode.
 */
static struct pnlfs_inode_info *get_pnlfs_inode_info(struct inode *inode);

/**
 * Get the &inod specified by @ino.
 */
static struct inode *pnlfs_iget(struct super_block *sb, unsigned long ino);

/**
 * Get the ino specified by the name @child in the directory @dir.
 */
ino_t pnlfs_inode_by_name(struct inode *dir, const struct qstr *child);

/**
 * Get the &dentry filled with the inod specified by @dentry->d_name.
 */
static struct dentry *pnlfs_lookup(struct inode *dir, struct dentry *dentry, 
				   unsigned int flags);

/**
 * Get the next free inode-id.
 */
static ino_t get_next_ifree(struct super_block *sb);

/**
 * Get the next free block-id.
 */
static uint32_t get_next_bfree(struct super_block *sb);

/**
 * Get a new inode.
 */
struct inode *pnlfs_new_inode(struct inode *dir, umode_t mode);

/**
 * Create a new file specified by @dentry in the directory @dir.
 */
static int pnlfs_create (struct inode *dir, struct dentry *dentry, 
			 umode_t mode, bool excl);

/**
 * Free the inode in the ifree bitmap specified by @ino.
 */
static void free_ifree(struct super_block *sb, ino_t ino);

/**
 * Free the block in the bfree bitmap specified by @block.
 */
static void free_bfree(struct super_block *sb, uint32_t block);

/**
 * Unlink the file specieid by @dentry in the directory @dir.
 */
static int pnlfs_unlink(struct inode *dir, struct dentry *dentry);

/**
 * Create a new directory specified by @dentry in the directory @dir.
 */
static int pnlfs_mkdir(struct inode *dir, struct dentry *dentry, umode_t mode);

/**
 * Remove the directory pecified by @dentry in the directory @dir.
 */
static int pnlfs_rmdir(struct inode *dir, struct dentry *dentry);

/**
 * Rename de 
 */
static int pnlfs_rename(struct inode *old_dir, struct dentry *old_dentry,
			struct inode *new_dir, struct dentry *new_dentry,
			unsigned int flags);

/**
 * Add the files in the directory @file to the context @ctx.
 */
static int pnlfs_readdir(struct file *file, struct dir_context *ctx);

/**
 * Dinamic allocation of a pnlfs_inode_info, and return the underlying &inode.
 */
static struct inode *pnlfs_alloc_inode(struct super_block *sb);

/**
 * Undo pnlfs_alloc_inode.
 */
static void pnlfs_destroy_inode(struct inode *inode);

/**
 * Write the @inode in the HDD.
 */
int pnlfs_write_inode(struct inode *inode, struct writeback_control *wbc);

/**
 * Initialize super block.
 */
static int pnlfs_fill_super(struct super_block *sb, void *data, int silent);

/**
 * Undo pnlfs_fill_super.
 */
static void pnlfs_put_super(struct super_block *sb);

/**
 * Write the super block and the bitmaps in HDD.
 */
static int pnlfs_sync_fs(struct super_block *sb, int wait);

/**
 * Mount the filesystem.
 */
static struct dentry *pnlfs_mount(struct file_system_type *fs_type, int flags, 
				  const char *dev_name, void *data);

//------------------------------------------------------------------------------
static const struct inode_operations pnlfs_iops = {
	.lookup = pnlfs_lookup,
	.create = pnlfs_create,
	.unlink = pnlfs_unlink,
	.mkdir = pnlfs_mkdir,
	.rmdir = pnlfs_rmdir,
	.rename = pnlfs_rename,
};

static const struct file_operations pnlfs_fops = {
	.iterate_shared	= pnlfs_readdir,
};

static const struct super_operations pnlfs_sops = {
	.alloc_inode = pnlfs_alloc_inode,
	.destroy_inode = pnlfs_destroy_inode,
	.write_inode = pnlfs_write_inode,
	.put_super = pnlfs_put_super,
	.sync_fs = pnlfs_sync_fs,
};

static struct file_system_type pnlfs_fs_type = {
	.owner = THIS_MODULE,
	.name = "pnlfs",
	.mount = pnlfs_mount,
	.kill_sb = kill_block_super,
	.fs_flags = FS_REQUIRES_DEV,
};

//------------------------------------------------------------------------------
static struct pnlfs_inode_info *get_pnlfs_inode_info(struct inode *inode)
{
	return (struct pnlfs_inode_info *)((void *)inode - 
		(void *)&((struct pnlfs_inode_info *)0)->vfs_inode);
}

static struct inode *pnlfs_iget(struct super_block *sb, unsigned long ino)
{
	struct inode *inode;
	struct buffer_head *bh;
	struct pnlfs_inode *pinode;
	struct pnlfs_inode_info *pinode_info;
	uint32_t cur_block;
	
	/* Get the inode. */
	inode = iget_locked(sb, ino);
	if (!inode)
		return NULL;
		
	/* Check whether or not the inode is new. */
	if (!(inode->i_state & I_NEW))
		return inode;
		
	/* Load the inode from driver. */
	cur_block = 1 + (ino / PNLFS_NR_INODES_IN_BLOCK);
	if (!(bh = sb_bread(sb, cur_block))) {
		pr_err("error: unable to read block");
		iget_failed(inode);
		return NULL;
	}
	pinode = (struct pnlfs_inode *) bh->b_data;
	pinode += ino % PNLFS_NR_INODES_IN_BLOCK;
	pinode_info = get_pnlfs_inode_info(inode);
	
	/* Initialize the inode. */
	inode->i_mode = le32_to_cpu(pinode->mode);
	inode->i_op = &pnlfs_iops;
	inode->i_fop = &pnlfs_fops;
	inode->i_sb = sb;
	inode->i_ino = ino;
	inode->i_size = le32_to_cpu(pinode->filesize);
	inode->i_blocks = S_ISDIR(pinode->mode) ? 1 : 
			  le32_to_cpu(pinode->nr_used_blocks) + 1;
	inode->i_atime = CURRENT_TIME;
	inode->i_mtime = CURRENT_TIME;
	inode->i_ctime = CURRENT_TIME;
	pinode_info->index_block = le32_to_cpu(pinode->index_block);
	pinode_info->nr_entries = le32_to_cpu(pinode->nr_entries);
	
	/* Clear the I_NEW flag. */
	unlock_new_inode(inode);
	
	brelse(bh);
	
	return inode;
}

ino_t pnlfs_inode_by_name(struct inode *dir, const struct qstr *child)
{
	const char *name = child->name;
	int namelen = child->len;
	struct buffer_head *bh;
	struct pnlfs_inode_info *pinode_info;
	struct pnlfs_dir_block *dir_block;
	int i;
	ino_t ino = 0;
	
	pinode_info = get_pnlfs_inode_info(dir);
	
	if (!(bh = sb_bread(dir->i_sb, pinode_info->index_block))) {
		pr_err("error: unable to read block");
		return 0;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	for (i = 0; i < pinode_info->nr_entries; ++i)
		if (!strncmp(name, dir_block->files[i].filename, namelen)) {
			ino = le32_to_cpu(dir_block->files[i].inode);
			break;
		}
	
	brelse(bh);
	
	return ino;
}

static struct dentry *pnlfs_lookup(struct inode *dir, struct dentry *dentry, 
				   unsigned int flags)
{
	struct inode *inode;
	ino_t ino;
	
	// TODO: check the name too long.
	
	ino = pnlfs_inode_by_name(dir, &dentry->d_name);
	if (!ino)
		goto failed;
	
	inode = pnlfs_iget(dir->i_sb, ino);
	if (!inode)
		goto failed;
	
	d_add(dentry, inode);
	
	return dentry;
	
failed:
	d_add(dentry, NULL);
	return dentry;
}

static ino_t get_next_ifree(struct super_block *sb)
{
	unsigned long *cur_ifree = ((struct pnlfs_sb_info *)sb->s_fs_info)
				   ->ifree_bitmap;
	struct pnlfs_sb_info *sbi = sb->s_fs_info;
	const unsigned long *last_ifree = sbi->ifree_bitmap + 
		(sbi->nr_ifree_blocks * PNLFS_NR_WORDS_IN_BLOCK);
	u8 bit_trg;
	
	if (sbi->nr_free_inodes == 0)
		return 0;
	
	while (*cur_ifree == 0)
	{
		++cur_ifree;
		
		if (cur_ifree == last_ifree)
			cur_ifree = sbi->ifree_bitmap;
	}
	
	for (bit_trg = 1; 
	     !(*cur_ifree & (1 << (bit_trg - 1)));
	     ++bit_trg)
		;
	
	*cur_ifree &= ~(1 << (bit_trg - 1));
	--sbi->nr_free_inodes;
	
	return (((void*)cur_ifree - (void*)sbi->ifree_bitmap) * 8) + 
		bit_trg - 1;
}

static uint32_t get_next_bfree(struct super_block *sb)
{
	unsigned long *cur_bfree = ((struct pnlfs_sb_info *)sb->s_fs_info)
				   ->bfree_bitmap;
	struct pnlfs_sb_info *sbi = sb->s_fs_info;
	const unsigned long *last_bfree = sbi->bfree_bitmap + 
		(sbi->nr_bfree_blocks * PNLFS_NR_WORDS_IN_BLOCK);
	u8 bit_trg;
	
	if (sbi->nr_free_blocks == 0)
		return 0;
	
	while (*cur_bfree == 0)
	{
		++cur_bfree;
		
		if (cur_bfree == last_bfree)
			cur_bfree = sbi->bfree_bitmap;
	}
	
	for (bit_trg = 1; 
	     !(*cur_bfree & (1 << (bit_trg - 1)));
	     ++bit_trg)
		;
	
	*cur_bfree &= ~(1 << (bit_trg - 1));
	--sbi->nr_free_blocks;
	
	return (((void*)cur_bfree - (void*)sbi->bfree_bitmap) * 8) + 
		bit_trg - 1;
}

struct inode *pnlfs_new_inode(struct inode *dir, umode_t mode)
{
	ino_t ino;
	struct inode *inode;
	struct pnlfs_inode_info *pinode_info;
	
	if ((ino = get_next_ifree(dir->i_sb)) == 0)
		return NULL;
	
	if ((inode = pnlfs_iget(dir->i_sb, ino)) == NULL)
		return NULL;
		
	pinode_info = get_pnlfs_inode_info(inode);
	
	inode->i_mode = mode;
	inode->i_size = 0;
	inode->i_blocks = 1;
	pinode_info->index_block = get_next_bfree(dir->i_sb);
	pinode_info->nr_entries = 0;
	mark_inode_dirty(inode);
	
	return inode;
}

static int pnlfs_create(struct inode *dir, struct dentry *dentry, 
			umode_t mode, bool excl)
{
	struct inode *inode;
	struct pnlfs_inode_info *dir_info;
	struct buffer_head *bh;
	struct pnlfs_dir_block *dir_block;
	
	// Get the directory inode.
	dir_info = get_pnlfs_inode_info(dir);
	if (dir_info->nr_entries >= PNLFS_MAX_DIR_ENTRIES)
		return -1;
	
	// Get the directory block.
	if (!(bh = sb_bread(dir->i_sb, dir_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	// Get a new inode.
	if ((inode = pnlfs_new_inode(dir, mode)) == NULL) {
		brelse(bh);
		return -1;
	}
	
	// Set data in the directory block.
	dir_block->files[dir_info->nr_entries].inode = 
		cpu_to_le32(inode->i_ino);
	strncpy(dir_block->files[dir_info->nr_entries].filename,
		dentry->d_name.name, dentry->d_name.len);
	mark_buffer_dirty(bh);
	
	// Update the directory inode.
	++dir_info->nr_entries;
	mark_inode_dirty(dir);
	
	// iput(inode);
	brelse(bh);
	
	return 0;
}

static void free_ifree(struct super_block *sb, ino_t ino)
{
	unsigned long *cur_ifree = ((struct pnlfs_sb_info *)sb->s_fs_info)
				   ->ifree_bitmap;
	
	cur_ifree[ino / PNLFS_WORD_SIZE] |= (1 << (ino % PNLFS_WORD_SIZE));
}

static void free_bfree(struct super_block *sb, uint32_t block)
{
	unsigned long *cur_bfree = ((struct pnlfs_sb_info *)sb->s_fs_info)
				   ->bfree_bitmap;
	
	cur_bfree[block / PNLFS_WORD_SIZE] |= (1 << (block % PNLFS_WORD_SIZE));
}

static int pnlfs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = dentry->d_inode;
	const char *name = dentry->d_name.name;
	int namelen = dentry->d_name.len;
	struct pnlfs_inode_info *inode_info;
	struct buffer_head *bh;
	struct pnlfs_dir_block *dir_block;
	struct pnlfs_file_index_block *file_index_block;
	int i;
	
	/* Check if it is a regular file. */
	if (!S_ISREG(inode->i_mode))
		return -1;
	
	/* Check if there are some entries in the directory. */
	inode_info = get_pnlfs_inode_info(dir);
	if (inode_info->nr_entries == 0)
		return -1;
	
	/* Get the directory block. */
	if (!(bh = sb_bread(dir->i_sb, inode_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	/* Delete directory entry. */
	for (i = 0; i < inode_info->nr_entries - 1; ++i)
		if (!strncmp(name, dir_block->files[i].filename, namelen)) {
			memcpy(&dir_block->files[i], 
			       &dir_block->files[i] + 1,
			       sizeof(struct pnlfs_file) * 
				       (inode_info->nr_entries - i - 1));
			break;
		}
	--inode_info->nr_entries;
	mark_inode_dirty(dir);
	mark_buffer_dirty(bh);
	brelse(bh);
	
	/* Get the regular file block. */
	inode_info = get_pnlfs_inode_info(inode);
	if (!(bh = sb_bread(inode->i_sb, inode_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	file_index_block = (struct pnlfs_file_index_block *) bh->b_data;
	
	/* Delete the regular file. */
	for (i = 0; i < inode_info->nr_entries; ++i)
		free_bfree(inode->i_sb,
			   le32_to_cpu(file_index_block->blocks[i]));
	free_bfree(inode->i_sb, inode_info->index_block);
	free_ifree(inode->i_sb, inode->i_ino);
	
	/* Unhashes the entry from the parent dentry hashes. */
	d_invalidate(dentry);
	brelse(bh);
	
	return 0;
}

static int pnlfs_mkdir(struct inode *dir, struct dentry *dentry, umode_t mode)
{
	struct inode *inode;
	struct pnlfs_inode_info *dir_info;
	struct buffer_head *bh;
	struct pnlfs_dir_block *dir_block;
	
	// Get the directory inode.
	dir_info = get_pnlfs_inode_info(dir);
	if (dir_info->nr_entries >= PNLFS_MAX_DIR_ENTRIES)
		return -1;
	
	// Get the directory block.
	if (!(bh = sb_bread(dir->i_sb, dir_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	// Get a new inode.
	if ((inode = pnlfs_new_inode(dir, S_IFDIR | mode)) == NULL) {
		brelse(bh);
		return -1;
	}
	
	// Set data in the directory block.
	dir_block->files[dir_info->nr_entries].inode = 
		cpu_to_le32(inode->i_ino);
	strncpy(dir_block->files[dir_info->nr_entries].filename,
		dentry->d_name.name, dentry->d_name.len);
	mark_buffer_dirty(bh);
	
	// Update the directory inode.
	++dir_info->nr_entries;
	mark_inode_dirty(dir);
	
	brelse(bh);
	
	return 0;
}

static int pnlfs_rmdir(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = dentry->d_inode;
	const char *name = dentry->d_name.name;
	int namelen = dentry->d_name.len;
	struct pnlfs_inode_info *dir_info = get_pnlfs_inode_info(dir);
	struct pnlfs_inode_info *inode_info = get_pnlfs_inode_info(inode);
	struct buffer_head *bh;
	struct pnlfs_dir_block *dir_block;
	int i;
	
	/* Check if the dentry points to a directory. */
	if (!S_ISDIR(inode->i_mode))
		return -1;
	
	/* Check if there are some entries in the directory. */
	if (dir_info->nr_entries == 0)
		return -1;
	
	/* Check if the dentry directory is empty. */
	if (inode_info->nr_entries != 0)
		return -1;
	
	/* Get the directory block. */
	if (!(bh = sb_bread(dir->i_sb, dir_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	/* Delete directory entry. */
	for (i = 0; i < dir_info->nr_entries - 1; ++i)
		if (!strncmp(name, dir_block->files[i].filename, namelen)) {
			memcpy(&dir_block->files[i], 
			       &dir_block->files[i] + 1,
			       sizeof(struct pnlfs_file) * 
				       (dir_info->nr_entries - i - 1));
			break;
		}
	--dir_info->nr_entries;
	mark_inode_dirty(dir);
	mark_buffer_dirty(bh);
	brelse(bh);
	
	/* Delete the dentry directory. */
	free_bfree(inode->i_sb, inode_info->index_block);
	free_ifree(inode->i_sb, inode->i_ino);
	
	/* Unhashes the entry from the parent dentry hashes. */
	inode->i_size = 0;
	inode_dec_link_count(inode);
	
	return 0;
}

static int pnlfs_rename(struct inode *old_dir, struct dentry *old_dentry,
			struct inode *new_dir, struct dentry *new_dentry,
			unsigned int flags)
{
	struct inode *old_inode = old_dentry->d_inode;
	struct inode *new_inode = new_dentry->d_inode;
	const char *old_name = old_dentry->d_name.name;
	const char *new_name = new_dentry->d_name.name;
	int old_namelen = old_dentry->d_name.len;
	// int new_namelen = new_dentry->d_name.len;
	struct pnlfs_inode_info *inode_info;
	struct buffer_head *bh;
	struct pnlfs_dir_block *dir_block;
	int i;
	
	if (new_inode == NULL)
		new_inode = old_inode;
	
	/* Check if there are some entries in the directory. */
	inode_info = get_pnlfs_inode_info(old_dir);
	if (inode_info->nr_entries == 0)
		return -1;
	
	/* Get the directory block. */
	if (!(bh = sb_bread(old_dir->i_sb, inode_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	/* Remove the directory entry. */
	for (i = 0; i < inode_info->nr_entries - 1; ++i)
		if (!strncmp(old_name, dir_block->files[i].filename, 
			     old_namelen)) {
			memcpy(&dir_block->files[i], 
			       &dir_block->files[i] + 1,
			       sizeof(struct pnlfs_file) * 
				       (inode_info->nr_entries - i - 1));
			break;
		}
	--inode_info->nr_entries;
	mark_inode_dirty(old_dir);
	mark_buffer_dirty(bh);
	d_invalidate(old_dentry);
	brelse(bh);
	
	// Get the directory inode.
	inode_info = get_pnlfs_inode_info(new_dir);
	if (inode_info->nr_entries >= PNLFS_MAX_DIR_ENTRIES)
		return -1;
	
	// Get the directory block.
	if (!(bh = sb_bread(new_dir->i_sb, inode_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	// Set data in the directory block.
	dir_block->files[inode_info->nr_entries].inode = 
		cpu_to_le32(new_inode->i_ino);
	strcpy(dir_block->files[inode_info->nr_entries].filename,
		new_name);
	mark_buffer_dirty(bh);
	
	// Update the directory inode.
	++inode_info->nr_entries;
	mark_inode_dirty(new_dir);
	brelse(bh);
	
	return 0;
}

static int pnlfs_readdir(struct file *file, struct dir_context *ctx)
{
	loff_t pos = ctx->pos;
	struct inode *inode = file->f_inode;
	struct pnlfs_inode_info *pinode_info = get_pnlfs_inode_info(inode);
	struct super_block *sb = inode->i_sb;
	struct buffer_head *bh;
	struct pnlfs_dir_block *dir_block;
	struct pnlfs_file *fle;
	unsigned char d_type;
	int i;
	
	if (pos >= pinode_info->nr_entries + 2)
		return 0;

	// TODO: the dots doesn't appear with ls -la.
	if (!dir_emit_dots(file, ctx))
		return 0;
	
	if (!(bh = sb_bread(inode->i_sb, pinode_info->index_block))) {
		pr_err("error: unable to read block");
		return -1;
	}
	dir_block = (struct pnlfs_dir_block *) bh->b_data;
	
	for (i = 0; i < pinode_info->nr_entries; ++i) {
		fle = &dir_block->files[i];
		inode = pnlfs_iget(sb, le32_to_cpu(fle->inode));
		d_type = S_ISDIR(inode->i_mode) ? DT_DIR : 
			 S_ISREG(inode->i_mode) ? DT_REG : DT_UNKNOWN;
		
		if (!dir_emit(ctx, fle->filename,
			      strnlen(fle->filename, PNLFS_FILENAME_LEN),
			      inode->i_ino, d_type)) {
			// iput(inode);
			brelse(bh);
			return 0;
		}
		
		ctx->pos += 1;
		// iput(inode);
	}
	
	brelse(bh);
	
	return 0;
}

static struct inode *pnlfs_alloc_inode(struct super_block *sb)
{
	struct pnlfs_inode_info *pi;
	
	pi = kzalloc(sizeof(*pi), GFP_KERNEL);
	if (!pi)
		return NULL;
		
	inode_init_once(&pi->vfs_inode);
	
	return &pi->vfs_inode;
}

static void pnlfs_destroy_inode(struct inode *inode)
{
	kfree(get_pnlfs_inode_info(inode));
}

int pnlfs_write_inode(struct inode *inode, struct writeback_control *wbc)
{
	struct pnlfs_inode_info *pinode_info;
	struct buffer_head *bh;
	struct pnlfs_inode *pinode;
	uint32_t block_trg;
	uint16_t inode_trg;
	
	pinode_info = get_pnlfs_inode_info(inode);
	
	/* Compute the block target. */
	block_trg = 1 + (inode->i_ino / PNLFS_NR_INODES_IN_BLOCK);
	
	/* Get the block target. */
	if (!(bh = sb_bread(inode->i_sb, block_trg))) {
		pr_err("error: unable to read block");
		return -1;
	}
	pinode = (struct pnlfs_inode *) bh->b_data;
	
	/* Compute the inode target. */
	inode_trg = inode->i_ino % PNLFS_NR_INODES_IN_BLOCK;
	pinode = &pinode[inode_trg];
	
	/* Write data in inode store. */
	pinode->mode = cpu_to_le32(inode->i_mode);
	pinode->index_block = cpu_to_le32(pinode_info->index_block);
	pinode->filesize = cpu_to_le32(inode->i_size);
	pinode->nr_entries = cpu_to_le32(pinode_info->nr_entries);
	mark_buffer_dirty(bh);
	sync_dirty_buffer(bh);
	
	brelse(bh);
	
	return 0;
}

static int pnlfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct pnlfs_sb_info *sbi;
	struct buffer_head *bh;
	struct pnlfs_superblock *psb;
	uint32_t cur_block;
	uint32_t last_block;
	__le64 *raw_world;
	unsigned long *cur_bitmap;
	unsigned long *last_bitmap;
	struct inode *inode;
	
	/* Allocate memory for the specific FS. */
	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		goto failed;
		
	/* Superblock initialization. */
	sb->s_magic = PNLFS_MAGIC;
	sb->s_blocksize = PNLFS_BLOCK_SIZE;
	sb->s_maxbytes = PNLFS_MAX_FILESIZE;
	sb->s_fs_info = sbi;
	sb->s_op = &pnlfs_sops;
	
	/* Read Superblock from driver. */
	if (!(bh = sb_bread(sb, PNLFS_SB_BLOCK_NR))) {
		pr_err("error: unable to read superblock");
		goto failed_sbi;
	}
	psb = (struct pnlfs_superblock *) bh->b_data;
	
	/* Superblock initialization continue. */
	sbi->nr_blocks = le32_to_cpu(psb->nr_blocks);
	sbi->nr_inodes = le32_to_cpu(psb->nr_inodes);
	sbi->nr_istore_blocks = le32_to_cpu(psb->nr_istore_blocks);
	sbi->nr_ifree_blocks = le32_to_cpu(psb->nr_ifree_blocks);
	sbi->nr_bfree_blocks = le32_to_cpu(psb->nr_bfree_blocks);
	sbi->nr_free_inodes = le32_to_cpu(psb->nr_free_inodes);
	sbi->nr_free_blocks = le32_to_cpu(psb->nr_free_blocks);
	brelse(bh);
	
	/* Allocate memory for inode_free_bitmap. */
	sbi->ifree_bitmap = kzalloc(PNLFS_BLOCK_SIZE * sbi->nr_ifree_blocks, 
				    GFP_KERNEL);
	if (!sbi->ifree_bitmap)
		goto failed_sbi;
	
	/* Read inode_free_bitmap from driver. */
	cur_block = 1 + sbi->nr_istore_blocks;
	last_block = cur_block + sbi->nr_ifree_blocks;
	cur_bitmap = sbi->ifree_bitmap;
	for (; cur_block < last_block; ++cur_block) {
		if (!(bh = sb_bread(sb, cur_block))) {
			pr_err("error: unable to read inode free bitmap");
			goto failed_ifree_bitmap;
		}
		raw_world = (__le64 *) bh->b_data;
		last_bitmap = cur_bitmap + PNLFS_NR_WORDS_IN_BLOCK;
		while (cur_bitmap != last_bitmap) {
			*cur_bitmap++ = le64_to_cpu(*raw_world);
			++raw_world;
		}
		brelse(bh);
	}
	
	/* Allocate memory for block_free_bitmap. */
	sbi->bfree_bitmap = kzalloc(PNLFS_BLOCK_SIZE * sbi->nr_bfree_blocks, 
				    GFP_KERNEL);
	if (!sbi->bfree_bitmap)
		goto failed_ifree_bitmap;
	
	/* Read block_free_bitmap from driver. */
	/* cur_block is actually in the good spot. */
	last_block = cur_block + sbi->nr_bfree_blocks;
	cur_bitmap = sbi->bfree_bitmap;
	for (; cur_block < last_block; ++cur_block) {
		if (!(bh = sb_bread(sb, cur_block))) {
			pr_err("error: unable to read block free bitmap");
			goto failed_bfree_bitmap;
		}
		raw_world = (__le64 *) bh->b_data;
		last_bitmap = cur_bitmap + PNLFS_NR_WORDS_IN_BLOCK;
		while (cur_bitmap != last_bitmap) {
			*cur_bitmap++ = le64_to_cpu(*raw_world);
			++raw_world;
		}
		brelse(bh);
	}
	
	inode = pnlfs_iget(sb, 0);
	inode_init_owner(inode, NULL, inode->i_mode);
	// TODO: s_root is never free ?
	sb->s_root = d_make_root(inode);
	// iput(inode);
	
	return 0;
	
failed_bfree_bitmap:
	kfree(sbi->bfree_bitmap);
failed_ifree_bitmap:
	kfree(sbi->ifree_bitmap);
failed_sbi:
	kfree(sbi);
	sb->s_fs_info = NULL;
failed:
	return -1;
}

static void pnlfs_put_super(struct super_block *sb)
{
	struct pnlfs_sb_info *sb_info = sb->s_fs_info;
	
	kfree(sb_info->ifree_bitmap);
	kfree(sb_info->bfree_bitmap);
	kfree(sb_info);
}

static int pnlfs_sync_fs(struct super_block *sb, int wait)
{
	struct pnlfs_sb_info *sb_info = sb->s_fs_info;
	struct buffer_head *bh;
	struct pnlfs_superblock *superblock;
	uint32_t cur_block;
	uint32_t last_block;
	__le64 *raw_world;
	unsigned long *cur_bitmap;
	unsigned long *last_bitmap;
	
	/* Get the superblock block. */
	if (!(bh = sb_bread(sb, PNLFS_SB_BLOCK_NR))) {
		pr_err("error: unable to read superblock");
		return -1;
	}
	superblock = (struct pnlfs_superblock *) bh->b_data;
	
	/* Update data in superblock. */
	superblock->nr_free_inodes = cpu_to_le32(sb_info->nr_free_inodes);
	superblock->nr_free_blocks = cpu_to_le32(sb_info->nr_free_blocks);
	mark_buffer_dirty(bh);
	sync_dirty_buffer(bh);
	brelse(bh);
	
	/* Update inode_free_bitmap. */
	cur_block = 1 + sb_info->nr_istore_blocks;
	last_block = cur_block + sb_info->nr_ifree_blocks;
	cur_bitmap = sb_info->ifree_bitmap;
	for (; cur_block < last_block; ++cur_block) {
		if (!(bh = sb_bread(sb, cur_block))) {
			pr_err("error: unable to read inode free bitmap");
			return -1;
		}
		raw_world = (__le64 *) bh->b_data;
		last_bitmap = cur_bitmap + PNLFS_NR_WORDS_IN_BLOCK;
		while (cur_bitmap != last_bitmap) {
			*raw_world++ = cpu_to_le64(*cur_bitmap);
			++cur_bitmap;
		}
		mark_buffer_dirty(bh);
		sync_dirty_buffer(bh);
		brelse(bh);
	}
	
	/* Update block_free_bitmap. */
	last_block = cur_block + sb_info->nr_bfree_blocks;
	cur_bitmap = sb_info->bfree_bitmap;
	for (; cur_block < last_block; ++cur_block) {
		if (!(bh = sb_bread(sb, cur_block))) {
			pr_err("error: unable to read block free bitmap");
			return -1;
		}
		raw_world = (__le64 *) bh->b_data;
		last_bitmap = cur_bitmap + PNLFS_NR_WORDS_IN_BLOCK;
		while (cur_bitmap != last_bitmap) {
			*raw_world++ = cpu_to_le64(*cur_bitmap);
			++cur_bitmap;
		}
		mark_buffer_dirty(bh);
		sync_dirty_buffer(bh);
		brelse(bh);
	}
	
	return 0;
}

static struct dentry *pnlfs_mount(struct file_system_type *fs_type, int flags, 
				  const char *dev_name, void *data)
{
	return mount_bdev(fs_type, flags, dev_name, data, pnlfs_fill_super);
}

static int __init pnlfs_init(void)
{
	int err;
	
	err = register_filesystem(&pnlfs_fs_type);
	
	if (err)
		return err;
	
	return 0;
}

static void __exit pnlfs_exit(void)
{	
	unregister_filesystem(&pnlfs_fs_type);
}

module_init(pnlfs_init);
module_exit(pnlfs_exit);
