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
#include <linux/debugfs.h>
#include<linux/kthread.h>
#include <linux/poll.h>
#include<linux/delay.h> 

#include <linux/uaccess.h>
#include <linux/ring-buf.h>

static void advance_pointer(ring_handle_t rbuf)
{
	BUG_ON(!rbuf);

	if(ring_buf_full(rbuf)) {
        if(++(rbuf->tail) == rbuf->capacity) {
            rbuf->tail = 0;
        }
    }

	if(++(rbuf->head) == rbuf->capacity) {
        rbuf->head = 0;
    }
}

static void retreat_pointer(ring_handle_t rbuf)
{
	BUG_ON(!rbuf);

	if(++(rbuf->tail) == rbuf->capacity) {
        rbuf->tail = 0;
    }
}

ring_handle_t ring_buf_init(uint64_t** buffer, size_t size)
{
	BUG_ON(!(buffer && size));

	ring_handle_t rbuf = kmalloc(sizeof(ring_buf_t), GFP_KERNEL);
	BUG_ON(!rbuf);

    spin_lock_init(&rbuf->ring_buf_lock);
	rbuf->buffer = buffer;
	rbuf->capacity = size;
	ring_buf_reset(rbuf);

	BUG_ON(!(ring_buf_empty(rbuf)));

	return rbuf;
}

void ring_buf_free(ring_handle_t rbuf)
{
	BUG_ON(!rbuf);
	kfree(rbuf);
}

void ring_buf_reset(ring_handle_t rbuf)
{
	BUG_ON(!rbuf);

	rbuf->head = 0;
	rbuf->tail = 0;
}

size_t ring_buf_size(ring_handle_t rbuf)
{
	BUG_ON(!rbuf);

	size_t size = rbuf->capacity;

	if(!ring_buf_full(rbuf)) {
        if(rbuf->head >= rbuf->tail) {
            size = (rbuf->head - rbuf->tail);
        }
        else {
            size = (rbuf->capacity + rbuf->head - rbuf->tail);
        }
    }

	return size;
}

size_t ring_buf_capacity(ring_handle_t rbuf)
{
	BUG_ON(!rbuf);
	return rbuf->capacity;
}

void ring_buf_put(ring_handle_t rbuf, uint64_t* data)
{
	BUG_ON(!(rbuf && rbuf->buffer));

    spin_lock(&rbuf->ring_buf_lock);

	rbuf->buffer[rbuf->head] = data;

	advance_pointer(rbuf);

    spin_unlock(&rbuf->ring_buf_lock);
}

int ring_buf_put2(ring_handle_t rbuf, uint64_t* data)
{
	int r = -1;

	BUG_ON(!(rbuf && rbuf->buffer));

	if(!ring_buf_full(rbuf)) {
        rbuf->buffer[rbuf->head] = data;
        advance_pointer(rbuf);
        r = 0;
	}

	return r;
}

uint64_t* ring_buf_get(ring_handle_t rbuf)
{
	BUG_ON(!(rbuf && rbuf->buffer));
	uint64_t* cur_read;

    spin_lock(&rbuf->ring_buf_lock);

	if(!ring_buf_empty(rbuf)) {
        cur_read = rbuf->buffer[rbuf->tail];
        retreat_pointer(rbuf);

        spin_unlock(&rbuf->ring_buf_lock);
        return cur_read;
    }

    spin_unlock(&rbuf->ring_buf_lock);
	return NULL;
}

bool ring_buf_empty(ring_handle_t rbuf)
{
	BUG_ON(!rbuf);
	return (!ring_buf_full(rbuf) && (rbuf->head == rbuf->tail));
}

bool ring_buf_full(ring_buf_t* rbuf)
{
	size_t head = rbuf->head + 1;
	if(head == rbuf->capacity) {
        head = 0;
	}

	return head == rbuf->tail;
}
