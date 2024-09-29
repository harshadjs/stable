// SPDX-License-Identifier: GPL-2.0-only
/*
 * Ram backed block device driver.
 *
 * Copyright (C) 2007 Nick Piggin
 * Copyright (C) 2007 Novell Inc.
 *
 * Parts derived from drivers/block/rd.c, and drivers/block/loop.c, copyright
 * of their respective owners.
 */

#include <linux/init.h>
#include <linux/initrd.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/major.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/highmem.h>
#include <linux/wait.h>
#include <linux/delay.h>
#include <linux/mutex.h>
#include <linux/pagemap.h>
#include <linux/xarray.h>
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/anon_inodes.h>
#include <linux/backing-dev.h>
#include <linux/ktime.h>
#include <linux/debugfs.h>
#include<linux/kthread.h>
#include <linux/poll.h>
#include<linux/delay.h> 
#include <linux/ring-buf.h>

#include <linux/uaccess.h>

static struct task_struct *dispatch_thread;
static struct task_struct *handle_user_thread;
static atomic_t num_inflight_bios;
static atomic_t last_lba;
static ring_handle_t submit_bio_free_indices;
static struct bio **submitted_bios;
static struct list_head predicted_bio_list;
static spinlock_t predicted_bios_list_lock;
static ring_handle_t predicted_bio_free_list;

struct predicted_bio {
	struct bio *bio;
	struct list_head list_entry;
};

struct user_bio_ctx {
	/* waitqueue head for the pending (i.e. not read) userfaults */
	wait_queue_head_t bio_pending_wqh;
	/* waitqueue head for the userfaults */
	wait_queue_head_t bio_wqh;
	/* waitqueue head for the pseudo fd to wakeup poll/read */
	wait_queue_head_t fd_wqh;
	/* waitqueue head for events */
	wait_queue_head_t event_wqh;
	/* a refile sequence protected by bio_pending_wqh lock */
	seqcount_spinlock_t refile_seq;
	/* pseudo fd refcounting */
	refcount_t refcount;
};

struct user_bio_wait_queue {
	struct user_bio_msg msg;
	wait_queue_entry_t wq;
	struct user_bio_ctx *ctx;
	bool waken;
};


/*
 * Each block ramdisk device has a xarray brd_pages of pages that stores
 * the pages containing the block device's contents. A brd page's ->index is
 * its offset in PAGE_SIZE units. This is similar to, but in no way connected
 * with, the kernel's pagecache or buffer cache (which sit above our block
 * device).
 */
struct brd_device {
	int			brd_number;
	struct gendisk		*brd_disk;
	struct list_head	brd_list;
	// struct outstanding_bio	obios[256];
	int			num_obios;
	int			ubio_pid;
	bool 			latency_injection_enabled;
	ring_handle_t	submit_ring;
	struct user_bio_ctx *ubio_ctx;

	/*
	 * Backing store of pages. This is the contents of the block device.
	 */
	struct xarray	        brd_pages;
	u64			brd_nr_pages;
};

/*
 * Look up and return a brd's page for a given sector.
 */
static struct page *brd_lookup_page(struct brd_device *brd, sector_t sector)
{
	pgoff_t idx;
	struct page *page;

	idx = sector >> PAGE_SECTORS_SHIFT; /* sector to page index */
	page = xa_load(&brd->brd_pages, idx);

	BUG_ON(page && page->index != idx);

	return page;
}

/*
 * Insert a new page for a given sector, if one does not already exist.
 */
static int brd_insert_page(struct brd_device *brd, sector_t sector, gfp_t gfp)
{
	pgoff_t idx;
	struct page *page, *cur;
	int ret = 0;

	page = brd_lookup_page(brd, sector);
	if (page)
		return 0;

	page = alloc_page(gfp | __GFP_ZERO | __GFP_HIGHMEM);
	if (!page)
		return -ENOMEM;

	xa_lock(&brd->brd_pages);

	idx = sector >> PAGE_SECTORS_SHIFT;
	page->index = idx;

	cur = __xa_cmpxchg(&brd->brd_pages, idx, NULL, page, gfp);

	if (unlikely(cur)) {
		__free_page(page);
		ret = xa_err(cur);
		if (!ret && (cur->index != idx))
			ret = -EIO;
	} else {
		brd->brd_nr_pages++;
	}

	xa_unlock(&brd->brd_pages);

	return ret;
}

/*
 * Free all backing store pages and xarray. This must only be called when
 * there are no other users of the device.
 */
static void brd_free_pages(struct brd_device *brd)
{
	struct page *page;
	pgoff_t idx;

	xa_for_each(&brd->brd_pages, idx, page) {
		__free_page(page);
		cond_resched();
	}

	xa_destroy(&brd->brd_pages);
}

static struct user_bio_ctx *init_once_user_bio_ctx(void)
{
	struct user_bio_ctx *ctx = kmalloc(sizeof(struct user_bio_ctx), GFP_KERNEL);
	
	// (struct user_bio_ctx *) mem;

	init_waitqueue_head(&ctx->bio_pending_wqh);
	init_waitqueue_head(&ctx->bio_wqh);
	init_waitqueue_head(&ctx->event_wqh);
	init_waitqueue_head(&ctx->fd_wqh);
	seqcount_spinlock_init(&ctx->refile_seq, &ctx->bio_pending_wqh.lock);

	return ctx;
}

static void __wake_user_bio(struct user_bio_ctx *ctx)
{
	spin_lock_irq(&ctx->bio_pending_wqh.lock);
	/* wake all in the range and autoremove */
	if (waitqueue_active(&ctx->bio_pending_wqh))
		__wake_up_locked_key(&ctx->bio_pending_wqh, TASK_NORMAL,
				     NULL);
	if (waitqueue_active(&ctx->bio_wqh))
		__wake_up(&ctx->bio_wqh, TASK_NORMAL, 1, NULL);
	spin_unlock_irq(&ctx->bio_pending_wqh.lock);
}


/*
 * userfaultfd_wake may be used in combination with the
 * UFFDIO_*_MODE_DONTWAKE to wakeup userfaults in batches.
 */
static int wake_user_bio(struct user_bio_ctx *ctx)
{
	int ret;
	unsigned seq;
	bool need_wakeup;

	/*
	 * To be sure waitqueue_active() is not reordered by the CPU
	 * before the pagetable update, use an explicit SMP memory
	 * barrier here. PT lock release or mmap_read_unlock(mm) still
	 * have release semantics that can allow the
	 * waitqueue_active() to be reordered before the pte update.
	 */
	smp_mb();

	/*
	 * Use waitqueue_active because it's very frequent to
	 * change the address space atomically even if there are no
	 * userfaults yet. So we take the spinlock only when we're
	 * sure we've userfaults to wake.
	 */
	do {
		seq = read_seqcount_begin(&ctx->refile_seq);
		need_wakeup = waitqueue_active(&ctx->bio_pending_wqh) ||
			waitqueue_active(&ctx->bio_wqh);
		cond_resched();
	} while (read_seqcount_retry(&ctx->refile_seq, seq));;
	if (need_wakeup)
		__wake_user_bio(ctx);
	ret = 0;

out:
	return ret;
}

static void user_bio_event_complete(struct user_bio_ctx *ctx,
				       struct user_bio_wait_queue *ewq)
{
	// ewq->msg.event = 0;
	wake_up_locked(&ctx->event_wqh);
	__remove_wait_queue(&ctx->event_wqh, &ewq->wq);
}


static const struct block_device_operations brd_fops;
static const struct file_operations brd_file_ops;

static long brd_file_ioctl(struct file *file, unsigned cmd,
			      unsigned long arg)
{
	int ret = -EINVAL;
	struct user_bio_ctx *ctx = file->private_data;
	struct bio *bio = NULL;

	// printk(KERN_INFO "in the brd file ioctl\n");
	int fd = 0;

	if (cmd == 0xabcd) {
		struct bio_latency {
			uint64_t bio_id;
			uint64_t latency_ns;
		} __attribute__((packed));
		struct bio_latency bio_lat;
		ret = copy_from_user(&bio_lat, (struct bio_latency*)arg, sizeof(struct bio_latency));
		// printk(KERN_INFO "predicted latency = %lu\n", bio_lat.latency_ns);
		bio = submitted_bios[bio_lat.bio_id];
		bio->predicted_latency_ns = bio_lat.latency_ns;
		submitted_bios[bio_lat.bio_id] = NULL;
		ring_buf_put(submit_bio_free_indices, (uint64_t*)bio_lat.bio_id);
		wake_user_bio(ctx);
	}
	return 0;
}

/* fault_pending_wqh.lock must be hold by the caller */
static inline struct user_bio_wait_queue *find_user_bio_in(
		wait_queue_head_t *wqh)
{
	wait_queue_entry_t *wq;
	struct user_bio_wait_queue *uwq;

	lockdep_assert_held(&wqh->lock);

	uwq = NULL;
	if (!waitqueue_active(wqh))
		goto out;
	/* walk in reverse to provide FIFO behavior to read userfaults */
	wq = list_last_entry(&wqh->head, typeof(*wq), entry);
	uwq = container_of(wq, struct user_bio_wait_queue, wq);
out:
	return uwq;
}

static inline struct user_bio_wait_queue *find_user_bio(
		struct user_bio_ctx *ctx)
{
	return find_user_bio_in(&ctx->bio_pending_wqh);
}

static inline struct user_bio_wait_queue *find_user_bio_evt(
		struct user_bio_ctx *ctx)
{
	return find_user_bio_in(&ctx->event_wqh);
}

static ssize_t user_bio_ctx_read(struct user_bio_ctx *ctx, int no_wait,
				    struct user_bio_msg *msg, struct inode *inode)
{
	ssize_t ret;
	DECLARE_WAITQUEUE(wait, current);
	struct user_bio_wait_queue *uwq;
	/*
	 * Handling fork event requires sleeping operations, so
	 * we drop the event_wqh lock, then do these ops, then
	 * lock it back and wake up the waiter. While the lock is
	 * dropped the ewq may go away so we keep track of it
	 * carefully.
	 */
	LIST_HEAD(fork_event);
	struct user_bio_ctx *fork_nctx = NULL;

	/* always take the fd_wqh lock before the fault_pending_wqh lock */
	spin_lock_irq(&ctx->fd_wqh.lock);
	__add_wait_queue(&ctx->fd_wqh, &wait);
	for (;;) {
		set_current_state(TASK_INTERRUPTIBLE);
		spin_lock(&ctx->bio_pending_wqh.lock);
		uwq = find_user_bio(ctx);
		if (uwq) {
			/*
			 * Use a seqcount to repeat the lockless check
			 * in wake_userfault() to avoid missing
			 * wakeups because during the refile both
			 * waitqueue could become empty if this is the
			 * only userfault.
			 */
			write_seqcount_begin(&ctx->refile_seq);

			/*
			 * The fault_pending_wqh.lock prevents the uwq
			 * to disappear from under us.
			 *
			 * Refile this userfault from
			 * fault_pending_wqh to fault_wqh, it's not
			 * pending anymore after we read it.
			 *
			 * Use list_del() by hand (as
			 * userfaultfd_wake_function also uses
			 * list_del_init() by hand) to be sure nobody
			 * changes __remove_wait_queue() to use
			 * list_del_init() in turn breaking the
			 * !list_empty_careful() check in
			 * handle_userfault(). The uwq->wq.head list
			 * must never be empty at any time during the
			 * refile, or the waitqueue could disappear
			 * from under us. The "wait_queue_head_t"
			 * parameter of __remove_wait_queue() is unused
			 * anyway.
			 */
			list_del(&uwq->wq.entry);
			add_wait_queue(&ctx->bio_wqh, &uwq->wq);

			write_seqcount_end(&ctx->refile_seq);

			/* careful to always initialize msg if ret == 0 */
			*msg = uwq->msg;
			spin_unlock(&ctx->bio_pending_wqh.lock);
			ret = 0;
			break;
		}
		spin_unlock(&ctx->bio_pending_wqh.lock);

		spin_lock(&ctx->event_wqh.lock);
		uwq = find_user_bio_evt(ctx);
		if (uwq) {
			*msg = uwq->msg;
			user_bio_event_complete(ctx, uwq);
			spin_unlock(&ctx->event_wqh.lock);
			ret = 0;
			break;
		}
		spin_unlock(&ctx->event_wqh.lock);

		if (signal_pending(current)) {
			ret = -ERESTARTSYS;
			break;
		}
		if (no_wait) {
			ret = -EAGAIN;
			break;
		}
		spin_unlock_irq(&ctx->fd_wqh.lock);
		schedule();
		spin_lock_irq(&ctx->fd_wqh.lock);
	}
	__remove_wait_queue(&ctx->fd_wqh, &wait);
	__set_current_state(TASK_RUNNING);
	spin_unlock_irq(&ctx->fd_wqh.lock);
	return ret;
}


static ssize_t user_bio_read(struct file *file, char __user *buf,
				size_t count, loff_t *ppos)
{
	struct user_bio_ctx *ctx = file->private_data;
	ssize_t _ret, ret = 0;
	struct user_bio_msg msg;
	int no_wait = file->f_flags & O_NONBLOCK;
	struct inode *inode = file_inode(file);

	for (;;) {
		if (count < sizeof(msg))
			return ret ? ret : -EINVAL;
		_ret = user_bio_ctx_read(ctx, no_wait, &msg, inode);
		if (_ret < 0)
			return ret ? ret : _ret;
		if (copy_to_user((__u64 __user *) buf, &msg, sizeof(msg)))
			return ret ? ret : -EFAULT;
		ret += sizeof(msg);
		buf += sizeof(msg);
		count -= sizeof(msg);
		/*
		 * Allow to read more than one fault at time but only
		 * block if waiting for the very first one.
		 */
		no_wait = O_NONBLOCK;
	}
}


/*
 * copy_to_brd_setup must be called before copy_to_brd. It may sleep.
 */
static int copy_to_brd_setup(struct brd_device *brd, sector_t sector, size_t n,
			     gfp_t gfp)
{
	unsigned int offset = (sector & (PAGE_SECTORS-1)) << SECTOR_SHIFT;
	size_t copy;
	int ret;

	copy = min_t(size_t, n, PAGE_SIZE - offset);
	ret = brd_insert_page(brd, sector, gfp);
	if (ret)
		return ret;
	if (copy < n) {
		sector += copy >> SECTOR_SHIFT;
		ret = brd_insert_page(brd, sector, gfp);
	}
	return ret;
}

/*
 * Copy n bytes from src to the brd starting at sector. Does not sleep.
 */
static void copy_to_brd(struct brd_device *brd, const void *src,
			sector_t sector, size_t n)
{
	struct page *page;
	void *dst;
	unsigned int offset = (sector & (PAGE_SECTORS-1)) << SECTOR_SHIFT;
	size_t copy;

	copy = min_t(size_t, n, PAGE_SIZE - offset);
	page = brd_lookup_page(brd, sector);
	BUG_ON(!page);

	dst = kmap_atomic(page);
	memcpy(dst + offset, src, copy);
	kunmap_atomic(dst);

	if (copy < n) {
		src += copy;
		sector += copy >> SECTOR_SHIFT;
		copy = n - copy;
		page = brd_lookup_page(brd, sector);
		BUG_ON(!page);

		dst = kmap_atomic(page);
		memcpy(dst, src, copy);
		kunmap_atomic(dst);
	}
}

/*
 * Copy n bytes to dst from the brd starting at sector. Does not sleep.
 */
static void copy_from_brd(void *dst, struct brd_device *brd,
			sector_t sector, size_t n)
{
	struct page *page;
	void *src;
	unsigned int offset = (sector & (PAGE_SECTORS-1)) << SECTOR_SHIFT;
	size_t copy;

	copy = min_t(size_t, n, PAGE_SIZE - offset);
	page = brd_lookup_page(brd, sector);
	if (page) {
		src = kmap_atomic(page);
		memcpy(dst, src + offset, copy);
		kunmap_atomic(src);
	} else
		memset(dst, 0, copy);

	if (copy < n) {
		dst += copy;
		sector += copy >> SECTOR_SHIFT;
		copy = n - copy;
		page = brd_lookup_page(brd, sector);
		if (page) {
			src = kmap_atomic(page);
			memcpy(dst, src, copy);
			kunmap_atomic(src);
		} else
			memset(dst, 0, copy);
	}
}

/*
 * Process a single bvec of a bio.
 */
static int brd_do_bvec(struct brd_device *brd, struct page *page,
			unsigned int len, unsigned int off, blk_opf_t opf,
			sector_t sector)
{
	void *mem;
	int err = 0;

	if (op_is_write(opf)) {
		/*
		 * Must use NOIO because we don't want to recurse back into the
		 * block or filesystem layers from page reclaim.
		 */
		gfp_t gfp = opf & REQ_NOWAIT ? GFP_NOWAIT : GFP_NOIO;

		err = copy_to_brd_setup(brd, sector, len, gfp);
		if (err)
			goto out;
	}

	mem = kmap_atomic(page);
	if (!op_is_write(opf)) {
		copy_from_brd(mem + off, brd, sector, len);
		flush_dcache_page(page);
	} else {
		flush_dcache_page(page);
		copy_to_brd(brd, mem + off, sector, len);
	}
	kunmap_atomic(mem);

out:
	return err;
}

static inline void msg_init(struct user_bio_msg *msg)
{
	/*
	 * Must use memset to zero out the paddings or kernel data is
	 * leaked to userland.
	 */
	memset(msg, 0, sizeof(struct user_bio_msg));
}


static inline struct user_bio_msg user_bio_msg(bool write, uint64_t bio_id, uint64_t bio_size, int outstanding_bios, uint64_t lba_diff, uint64_t lba)
{
	struct user_bio_msg msg;
	msg_init(&msg);
	msg.write = true;
	msg.bio_id = bio_id;
	msg.bio_size = bio_size;
	msg.outstanding_bios = outstanding_bios;
	msg.lba_diff = lba_diff;
	msg.lba = lba;

	return msg;
}

static int user_bio_wake_function(wait_queue_entry_t *wq, unsigned mode,
				     int wake_flags, void *key)
{
	int ret;
	struct user_bio_wait_queue *uwq;

	uwq = container_of(wq, struct user_bio_wait_queue, wq);
	ret = 0;
	WRITE_ONCE(uwq->waken, true);
	/*
	 * The Program-Order guarantees provided by the scheduler
	 * ensure uwq->waken is visible before the task is woken.
	 */
	ret = wake_up_state(wq->private, mode);
	if (ret) {
		/*
		 * Wake only once, autoremove behavior.
		 *
		 * After the effect of list_del_init is visible to the other
		 * CPUs, the waitqueue may disappear from under us, see the
		 * !list_empty_careful() in handle_userfault().
		 *
		 * try_to_wake_up() has an implicit smp_mb(), and the
		 * wq->private is read before calling the extern function
		 * "wake_up_state" (which in turns calls try_to_wake_up).
		 */
		list_del_init(&wq->entry);
	}
out:
	return ret;
}


static void handle_user_bio(struct bio *bio)
{
	struct user_bio_wait_queue uwq;
	struct brd_device *brd = bio->bi_bdev->bd_disk->private_data;
	unsigned int blocking_state;
	struct user_bio_ctx *ctx = brd->ubio_ctx;
	bool must_wait;
	bool write;
	sector_t len;
	int submitted_bio_idx = 0;
	int num_inflight = 0;

	init_waitqueue_func_entry(&uwq.wq, user_bio_wake_function);
	uwq.wq.private = current;

	if (op_is_write(bio->bi_opf)) {
		write = true;
	} else {
		write = false;
	}
	num_inflight = atomic_read(&num_inflight_bios);

	len = bio_issue_size(&bio->bi_issue);

	submitted_bio_idx = (uint64_t)ring_buf_get(submit_bio_free_indices);	
	submitted_bios[submitted_bio_idx] = bio;

	uwq.msg = user_bio_msg(write, submitted_bio_idx, len, num_inflight, bio->lba_diff, bio->bi_iter.bi_sector);
	uwq.waken = false;
	uwq.ctx = ctx;

	// printk(KERN_INFO "%s: Going to userland\n", __func__);

	blocking_state = TASK_INTERRUPTIBLE;


	spin_lock_irq(&ctx->bio_pending_wqh.lock);

	/*
	 * After the __add_wait_queue the uwq is visible to userland
	 * through poll/read().
	 */
	__add_wait_queue(&ctx->bio_pending_wqh, &uwq.wq);
	/*
	 * The smp_mb() after __set_current_state prevents the reads
	 * following the spin_unlock to happen before the list_add in
	 * __add_wait_queue.
	 */
	set_current_state(blocking_state);
	spin_unlock_irq(&ctx->bio_pending_wqh.lock);

	must_wait = true;

	if (likely(must_wait)) {
		wake_up_poll(&ctx->fd_wqh, EPOLLIN);
		schedule();
	}

	__set_current_state(TASK_RUNNING);
	// printk(KERN_INFO "%s: Returned from userland\n", __func__);

	/*
	 * Here we race with the list_del; list_add in
	 * userfaultfd_ctx_read(), however because we don't ever run
	 * list_del_init() to refile across the two lists, the prev
	 * and next pointers will never point to self. list_add also
	 * would never let any of the two pointers to point to
	 * self. So list_empty_careful won't risk to see both pointers
	 * pointing to self at any time during the list refile. The
	 * only case where list_del_init() is called is the full
	 * removal in the wake function and there we don't re-list_add
	 * and it's fine not to block on the spinlock. The uwq on this
	 * kernel stack can be released after the list_del_init.
	 */
	if (!list_empty_careful(&uwq.wq.entry)) {
		spin_lock_irq(&ctx->bio_pending_wqh.lock);
		/*
		 * No need of list_del_init(), the uwq on the stack
		 * will be freed shortly anyway.
		 */
		list_del(&uwq.wq.entry);
		spin_unlock_irq(&ctx->bio_pending_wqh.lock);
	}
}

static __poll_t user_bio_poll(struct file *file, poll_table *wait)
{
	struct user_bio_ctx *ctx = file->private_data;
	__poll_t ret;

	poll_wait(file, &ctx->fd_wqh, wait);

	/*
	 * poll() never guarantees that read won't block.
	 * userfaults can be waken before they're read().
	 */
	if (unlikely(!(file->f_flags & O_NONBLOCK)))
		return EPOLLERR;
	/*
	 * lockless access to see if there are pending faults
	 * __pollwait last action is the add_wait_queue but
	 * the spin_unlock would allow the waitqueue_active to
	 * pass above the actual list_add inside
	 * add_wait_queue critical section. So use a full
	 * memory barrier to serialize the list_add write of
	 * add_wait_queue() with the waitqueue_active read
	 * below.
	 */
	ret = 0;
	smp_mb();
	if (waitqueue_active(&ctx->bio_pending_wqh))
		ret = EPOLLIN;
	else if (waitqueue_active(&ctx->event_wqh))
		ret = EPOLLIN;

	return ret;
}

int bio_dispatcher(void *priv)
{
	struct brd_device *brd = (struct brd_device *)priv;
	struct bio *bio = NULL;
	struct predicted_bio *new_bio = NULL;
	u64 current_time_us;
	u64 complete_time_us;
	u64 sleep_time;

	while (!kthread_should_stop()) {
		// msleep(5);
		new_bio = NULL;
		spin_lock(&predicted_bios_list_lock);
		new_bio = list_first_entry_or_null(&predicted_bio_list, struct predicted_bio, list_entry);
		if (new_bio != NULL) {
			list_del(&new_bio->list_entry);
		}
		spin_unlock(&predicted_bios_list_lock);
		if (!new_bio)
			continue;

		current_time_us = ktime_get_raw_ns() / 1000;
		complete_time_us = new_bio->bio->complete_time_ns / 1000;
		sleep_time = (current_time_us >= complete_time_us) ? 0 : complete_time_us - current_time_us;
		if (sleep_time > 0) 
			printk(KERN_INFO "%s: bio sleeping for %lu time. submit time = %lu, complete_time = %lu, current_time = %lu, latency = %lu\n", __func__, sleep_time, new_bio->bio->submit_time_ns / 1000, complete_time_us, current_time_us, new_bio->bio->predicted_latency_ns);
		usleep_range(sleep_time, sleep_time + 1);
		// usleep(sleep_time);
		bio_endio(new_bio->bio);
		ring_buf_put(predicted_bio_free_list, (uint64_t*)new_bio);
		atomic_dec(&num_inflight_bios);
	}
	pr_err("thread stopped\n");
	return 0;
}

int bio_user_handler(void *priv)
{
	struct brd_device *brd = (struct brd_device *)priv;
	struct bio *bio = NULL;
	struct predicted_bio *pos;
	struct predicted_bio *new_predicted_bio;

	while (!kthread_should_stop()) {
		// msleep(5);
		bio = (struct bio *)ring_buf_get(brd->submit_ring);		
		if (bio) {
			handle_user_bio(bio);
			// insert sleep here
			// bio_endio(bio);
			bio->complete_time_ns = bio->submit_time_ns + bio->predicted_latency_ns;
			new_predicted_bio = (struct predicted_bio *)ring_buf_get(predicted_bio_free_list);

			if (!new_predicted_bio)
				continue;

			INIT_LIST_HEAD(&new_predicted_bio->list_entry);
			new_predicted_bio->bio = bio;

			spin_lock(&predicted_bios_list_lock);
			list_for_each_entry(pos, &predicted_bio_list, list_entry) {
				if (pos->bio->complete_time_ns > bio->complete_time_ns) {
					list_add_tail(&new_predicted_bio->list_entry, &pos->list_entry);
					break;
				}
			}
			list_add_tail(&new_predicted_bio->list_entry, &predicted_bio_list);
			spin_unlock(&predicted_bios_list_lock);
		}
	}
	pr_err("thread stopped\n");
	return 0;
}


static void brd_submit_bio(struct bio *bio)
{
	struct brd_device *brd = bio->bi_bdev->bd_disk->private_data;
	sector_t sector = bio->bi_iter.bi_sector;
	struct bio_vec bvec;
	struct bvec_iter iter;
	uint64_t previous_lba;

	// dump_stack();
	bio->submit_time_ns = ktime_get_raw_ns();

	bio_for_each_segment(bvec, bio, iter) {
		unsigned int len = bvec.bv_len;
		int err;

		/* Don't support un-aligned buffer */
		WARN_ON_ONCE((bvec.bv_offset & (SECTOR_SIZE - 1)) ||
				(len & (SECTOR_SIZE - 1)));

		err = brd_do_bvec(brd, bvec.bv_page, len, bvec.bv_offset,
				  bio->bi_opf, sector);
		if (err) {
			if (err == -ENOMEM && bio->bi_opf & REQ_NOWAIT) {
				bio_wouldblock_error(bio);
				return;
			}
			bio_io_error(bio);
			return;
		}
		sector += len >> SECTOR_SHIFT;
	}

	if (!brd->latency_injection_enabled) {
		bio_endio(bio);
		return;
	}

	// TODO: Concurrency problems exist here.
	previous_lba = atomic_read(&last_lba);
	bio->lba_diff = (sector >= previous_lba) ? sector - previous_lba : previous_lba - sector;

	atomic_set(&last_lba, sector);
	atomic_inc(&num_inflight_bios);
	ring_buf_put(brd->submit_ring, (uint64_t *)bio);
}

static int brd_ioctl(struct block_device *bdev, blk_mode_t mode,
			unsigned int cmd, unsigned long arg)
{
	struct brd_device *brd = bdev->bd_disk->private_data;
	int fd = 0;
	uint64_t** buffer;
	struct predicted_bio *free_predicted_bio = NULL;
	// printk(KERN_INFO "in the brd dev ioctl\n");

	if (cmd == 0xcafe) {
		brd->latency_injection_enabled = true;
		brd->ubio_ctx = init_once_user_bio_ctx();
		brd->ubio_pid = current->pid;

		buffer = (uint64_t**)kmalloc(sizeof(uint64_t*) * 1024, GFP_KERNEL);
		submit_bio_free_indices = ring_buf_init(buffer, 1024);
		for (uint64_t i = 0; i < 1024; i++) {
			ring_buf_put(submit_bio_free_indices, (uint64_t*)i);
		}


		buffer = (uint64_t**)kmalloc(sizeof(struct predicted_bio *) * 1024, GFP_KERNEL);
		predicted_bio_free_list = ring_buf_init(buffer, 1024);
		for (uint64_t i = 0; i < 1024; i++) {
			free_predicted_bio = (struct predicted_bio *)kmalloc(sizeof(struct predicted_bio), GFP_KERNEL);
			free_predicted_bio->bio = NULL;
			INIT_LIST_HEAD(&free_predicted_bio->list_entry);
			ring_buf_put(predicted_bio_free_list, (uint64_t*)free_predicted_bio);
		}

		INIT_LIST_HEAD(&predicted_bio_list);
		spin_lock_init(&predicted_bios_list_lock);

		buffer = (uint64_t**)kmalloc(sizeof(uint64_t*) * 1024, GFP_KERNEL);
		brd->submit_ring = ring_buf_init(buffer, 1024);

		submitted_bios = (struct bio **)kmalloc(sizeof(struct bio *) * 1024, GFP_KERNEL);

		dispatch_thread = kthread_create(
					bio_dispatcher, brd,
					"mldisksim_dispatcher");
		if (dispatch_thread != NULL)
			wake_up_process(dispatch_thread);
		pr_err("mldisksim_dispatcher is running\n");

		handle_user_thread = kthread_create(
					bio_user_handler, brd,
					"mldisksim_user_handler");
		if (handle_user_thread != NULL)
			wake_up_process(handle_user_thread);
		pr_err("mldisksim_user_handler is running\n");

		// INIT_LIST_HEAD(&predicted_bio_list);
		// spin_lock_init(&predicted_bios_list_lock);

		fd = anon_inode_getfd_secure("[user_bio]", &brd_file_ops, brd->ubio_ctx,
			O_RDONLY | O_CLOEXEC | O_NONBLOCK, NULL);

		printk(KERN_INFO "%s: returning fd = %d\n", __func__, fd);
		return fd;
	} else if (cmd == 0xdead) {
		if (!brd->latency_injection_enabled)
			return -EINVAL;
		if (brd->num_obios == 0)
			return 0;
		// brd->num_obios--;
		// bio_endio(brd->obios[brd->num_obios].bio);
		return 0;
	} 
	return 0;
}


static const struct block_device_operations brd_fops = {
	.owner =		THIS_MODULE,
	.submit_bio =		brd_submit_bio,
	.ioctl =		brd_ioctl,
};

static const struct file_operations brd_file_ops = {
	.read = 		user_bio_read,
	.poll = 		user_bio_poll,
	.unlocked_ioctl = 		brd_file_ioctl,
};

/*
 * And now the modules code and kernel interface.
 */
static int rd_nr = CONFIG_BLK_DEV_RAM_COUNT;
module_param(rd_nr, int, 0444);
MODULE_PARM_DESC(rd_nr, "Maximum number of brd devices");

unsigned long rd_size = CONFIG_BLK_DEV_RAM_SIZE;
module_param(rd_size, ulong, 0444);
MODULE_PARM_DESC(rd_size, "Size of each RAM disk in kbytes.");

static int max_part = 1;
module_param(max_part, int, 0444);
MODULE_PARM_DESC(max_part, "Num Minors to reserve between devices");

MODULE_LICENSE("GPL");
MODULE_ALIAS_BLOCKDEV_MAJOR(RAMDISK_MAJOR);
MODULE_ALIAS("rd");

#ifndef MODULE
/* Legacy boot options - nonmodular */
static int __init ramdisk_size(char *str)
{
	rd_size = simple_strtol(str, NULL, 0);
	return 1;
}
__setup("ramdisk_size=", ramdisk_size);
#endif

/*
 * The device scheme is derived from loop.c. Keep them in synch where possible
 * (should share code eventually).
 */
static LIST_HEAD(brd_devices);
static struct dentry *brd_debugfs_dir;

static int brd_alloc(int i)
{
	struct brd_device *brd;
	struct gendisk *disk;
	char buf[DISK_NAME_LEN];
	int err = -ENOMEM;

	list_for_each_entry(brd, &brd_devices, brd_list)
		if (brd->brd_number == i)
			return -EEXIST;
	brd = kzalloc(sizeof(*brd), GFP_KERNEL);
	if (!brd)
		return -ENOMEM;
	brd->brd_number		= i;
	brd->latency_injection_enabled = false;
	brd->num_obios = 0;
	list_add_tail(&brd->brd_list, &brd_devices);

	xa_init(&brd->brd_pages);

	snprintf(buf, DISK_NAME_LEN, "ram%d", i);
	if (!IS_ERR_OR_NULL(brd_debugfs_dir))
		debugfs_create_u64(buf, 0444, brd_debugfs_dir,
				&brd->brd_nr_pages);

	disk = brd->brd_disk = blk_alloc_disk(NUMA_NO_NODE);
	if (!disk)
		goto out_free_dev;

	disk->major		= RAMDISK_MAJOR;
	disk->first_minor	= i * max_part;
	disk->minors		= max_part;
	disk->fops		= &brd_fops;
	disk->private_data	= brd;
	strscpy(disk->disk_name, buf, DISK_NAME_LEN);
	set_capacity(disk, rd_size * 2);
	
	/*
	 * This is so fdisk will align partitions on 4k, because of
	 * direct_access API needing 4k alignment, returning a PFN
	 * (This is only a problem on very small devices <= 4M,
	 *  otherwise fdisk will align on 1M. Regardless this call
	 *  is harmless)
	 */
	blk_queue_physical_block_size(disk->queue, PAGE_SIZE);

	/* Tell the block layer that this is not a rotational device */
	blk_queue_flag_set(QUEUE_FLAG_NONROT, disk->queue);
	blk_queue_flag_set(QUEUE_FLAG_SYNCHRONOUS, disk->queue);
	blk_queue_flag_set(QUEUE_FLAG_NOWAIT, disk->queue);
	err = add_disk(disk);
	if (err)
		goto out_cleanup_disk;

	return 0;

out_cleanup_disk:
	put_disk(disk);
out_free_dev:
	list_del(&brd->brd_list);
	kfree(brd);
	return err;
}

static void brd_probe(dev_t dev)
{
	brd_alloc(MINOR(dev) / max_part);
}

static void brd_cleanup(void)
{
	struct brd_device *brd, *next;

	debugfs_remove_recursive(brd_debugfs_dir);

	list_for_each_entry_safe(brd, next, &brd_devices, brd_list) {
		del_gendisk(brd->brd_disk);
		put_disk(brd->brd_disk);
		brd_free_pages(brd);
		list_del(&brd->brd_list);
		kfree(brd);
	}
}

static inline void brd_check_and_reset_par(void)
{
	if (unlikely(!max_part))
		max_part = 1;

	/*
	 * make sure 'max_part' can be divided exactly by (1U << MINORBITS),
	 * otherwise, it is possiable to get same dev_t when adding partitions.
	 */
	if ((1U << MINORBITS) % max_part != 0)
		max_part = 1UL << fls(max_part);

	if (max_part > DISK_MAX_PARTS) {
		pr_info("brd: max_part can't be larger than %d, reset max_part = %d.\n",
			DISK_MAX_PARTS, DISK_MAX_PARTS);
		max_part = DISK_MAX_PARTS;
	}
}

static int __init brd_init(void)
{
	int err, i;

	brd_check_and_reset_par();

	brd_debugfs_dir = debugfs_create_dir("ramdisk_pages", NULL);

	for (i = 0; i < rd_nr; i++) {
		err = brd_alloc(i);
		if (err)
			goto out_free;
	}



	/*
	 * brd module now has a feature to instantiate underlying device
	 * structure on-demand, provided that there is an access dev node.
	 *
	 * (1) if rd_nr is specified, create that many upfront. else
	 *     it defaults to CONFIG_BLK_DEV_RAM_COUNT
	 * (2) User can further extend brd devices by create dev node themselves
	 *     and have kernel automatically instantiate actual device
	 *     on-demand. Example:
	 *		mknod /path/devnod_name b 1 X	# 1 is the rd major
	 *		fdisk -l /path/devnod_name
	 *	If (X / max_part) was not already created it will be created
	 *	dynamically.
	 */

	if (__register_blkdev(RAMDISK_MAJOR, "ramdisk", brd_probe)) {
		err = -EIO;
		goto out_free;
	}

	pr_info("brd: module loaded\n");
	return 0;

out_free:
	brd_cleanup();

	pr_info("brd: module NOT loaded !!!\n");
	return err;
}

static void __exit brd_exit(void)
{

	unregister_blkdev(RAMDISK_MAJOR, "ramdisk");
	brd_cleanup();

	pr_info("brd: module unloaded\n");
}

module_init(brd_init);
module_exit(brd_exit);

