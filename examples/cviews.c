#include "config.h"
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <libcouchstore/couch_db.h>
#include <snappy-c.h>
#include "couch_btree.h"
#include "util.h"
#include "bitfield.h"
#include "internal.h"
#include "node_types.h"
#include "views/util.h"
#include "views/index_header.h"
#include "views/view_group.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <evhtp.h>

#define MAX_HEADER_SIZE (64 * 1024)

typedef struct {
    tree_file *file;
    uint64_t root_pointer;
} request_ctx;

typedef struct {
    evhtp_request_t *req;
    uint64_t limit;
} lookup_ctx;

const char * chunk_strings[] = {
    "I give you the light of Eärendil,\n",
    "our most beloved star.\n",
    "May it be a light for you in dark places,\n",
    "when all other lights go out.\n",
    NULL
};



static couchstore_error_t find_view_header_at_pos(view_group_info_t *info,
                                                cs_off_t pos)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    uint8_t buf;
    ssize_t readsize = info->file.ops->pread(&info->file.lastError,
                                            info->file.handle,
                                            &buf, 1, pos);
    error_unless(readsize == 1, COUCHSTORE_ERROR_READ);
    if (buf == 0) {
        return COUCHSTORE_ERROR_NO_HEADER;
    } else if (buf != 1) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    info->header_pos = pos;

    return COUCHSTORE_SUCCESS;

cleanup:
    return errcode;
}

static couchstore_error_t find_view_header(view_group_info_t *info,
                                        int64_t start_pos)
{
    couchstore_error_t last_header_errcode = COUCHSTORE_ERROR_NO_HEADER;
    int64_t pos = start_pos;
    pos -= pos % COUCH_BLOCK_SIZE;
    for (; pos >= 0; pos -= COUCH_BLOCK_SIZE) {
        couchstore_error_t errcode = find_view_header_at_pos(info, pos);
        switch(errcode) {
            case COUCHSTORE_SUCCESS:
                // Found it!
                return COUCHSTORE_SUCCESS;
            case COUCHSTORE_ERROR_NO_HEADER:
                // No header here, so keep going
                break;
            case COUCHSTORE_ERROR_ALLOC_FAIL:
                // Fatal error
                return errcode;
            default:
                // Invalid header; continue, but remember the last error
                last_header_errcode = errcode;
                break;
        }
    }
    return last_header_errcode;
}



static int view_btree_cmp(const sized_buf *key1, const sized_buf *key2)
{
    return view_key_cmp(key1, key2, NULL);
}


//static void printsb(const sized_buf *sb)
//{
//    if (sb->buf == NULL) {
//        printf("null\n");
//        return;
//    }
//    printf("%.*s\n", (int) sb->size, sb->buf);
//}


static couchstore_error_t lookup_callback(couchfile_lookup_request *rq,
                                          const sized_buf *k,
                                          const sized_buf *v)
{
    const uint16_t json_key_len = decode_raw16(*((raw_16 *) k->buf));
    sized_buf json_key;
    sized_buf json_value;

    json_key.buf = k->buf + sizeof(uint16_t);
    json_key.size = json_key_len;

    json_value.size = v->size - sizeof(raw_kv_length);
    json_value.buf = v->buf + sizeof(raw_kv_length);


    const char * chunk_str;
    evbuf_t    * buf;
    int          i = 0;
    lookup_ctx *ctx = rq->callback_ctx;
    //evhtp_request_t *req = rq->callback_ctx;

    buf = evbuffer_new();

    evbuffer_add(buf, json_key.buf, json_key.size);
    evbuffer_add(buf, json_value.buf, json_value.size);
    evbuffer_add(buf, "\n", 1);

    evhtp_send_reply_chunk(ctx->req, buf);

    evbuffer_drain(buf, -1);

    evbuffer_free(buf);

    ctx->limit--;
    if (ctx->limit > 0) {
      return 1;
    }
    else {
      return -1;
    }

// //   if (dumpJson) {
////        printf("{\"id\":\"");
////        printjquote(&json_key);
////        printf("\",\"data\":\"");
////        printjquote(&json_value);
////        printf("\"}\n");
////    } else {
//        printf("Doc ID: ");
//        printsb(&json_key);
//        printf("data: ");
//        printsb(&json_value);
////    }
// 
//    printf("\n");
    rq->num_keys++;

    return COUCHSTORE_SUCCESS;
}



int open_view_file(const char *filename, tree_file **file,
                   uint64_t *root_pointer)
{
    view_group_info_t *info;
    couchstore_error_t errcode;
    index_header_t *header = NULL;
    char *header_buf = NULL;
    int header_len;

    info = (view_group_info_t *)calloc(1, sizeof(view_group_info_t));
    if (info == NULL) {
        fprintf(stderr, "Unable to allocate memory\n");
        return -1;
    }
    info->type = VIEW_INDEX_TYPE_MAPREDUCE;

    errcode = open_view_group_file(filename, COUCHSTORE_OPEN_FLAG_RDONLY, &info->file);
    if (errcode != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Failed to open \"%s\": %s\n",
                filename, couchstore_strerror(errcode));
        return -1;
    } else {
        printf("Dumping \"%s\":\n", filename);
    }

    info->file.pos = info->file.ops->goto_eof(&info->file.lastError,
                                              info->file.handle);

    errcode = find_view_header(info, info->file.pos - 2);
    if (errcode != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Unable to find header position \"%s\": %s\n",
                filename, couchstore_strerror(errcode));
        return -1;
    }

    header_len = pread_header(&info->file, (cs_off_t)info->header_pos, &header_buf,
                            MAX_HEADER_SIZE);

    if (header_len < 0) {
        return -1;
    }

    errcode = decode_index_header(header_buf, (size_t) header_len, &header);
    free(header_buf);
    printf("Num views: %d\n", header->num_views);

    *root_pointer = header->view_states[0]->pointer;
    *file = &info->file;

    fflush(stderr);
    return 0;
}






void
testcb(evhtp_request_t * req, void * a) {
    const char * str = a;

    evbuffer_add(req->buffer_out, str, strlen(str));
    evhtp_send_reply(req, EVHTP_RES_OK);
}

void
issue161cb(evhtp_request_t * req, void * a) {
    struct evbuffer * b = evbuffer_new();

    if (evhtp_request_get_proto(req) == EVHTP_PROTO_10) {
        evhtp_request_set_keepalive(req, 0);
    }

    evhtp_send_reply_start(req, EVHTP_RES_OK);

    evbuffer_add(b, "foo", 3);
    evhtp_send_reply_body(req, b);

    evbuffer_add(b, "bar\n\n", 5);
    evhtp_send_reply_body(req, b);

    evhtp_send_reply_end(req);

    evbuffer_free(b);
}


static void
chunked(evhtp_request_t * req, void * arg) {
    const char * chunk_str;
    evbuf_t    * buf;
    int          i = 0;

    buf = evbuffer_new();

    evhtp_send_reply_chunk_start(req, EVHTP_RES_OK);

    while ((chunk_str = chunk_strings[i++]) != NULL) {
        evbuffer_add(buf, chunk_str, strlen(chunk_str));

        evhtp_send_reply_chunk(req, buf);

        evbuffer_drain(buf, -1);
    }

    evhtp_send_reply_chunk_end(req);
    evbuffer_free(buf);
}


static void
query(evhtp_request_t * req, void * arg) {
    request_ctx *ctx = (request_ctx *)arg;
    lookup_ctx lookup_ctx = {req, 5};

    couchstore_error_t errcode;
    sized_buf nullkey = {NULL, 0};
    sized_buf *lowkeys = &nullkey;
    couchfile_lookup_request rq;

    rq.cmp.compare = view_btree_cmp;
    rq.file = ctx->file;
    rq.num_keys = 1;
    rq.keys = &lowkeys;
    rq.callback_ctx = &lookup_ctx;
    rq.fetch_callback = lookup_callback;
    rq.node_callback = NULL;
    rq.fold = 1;

    evhtp_send_reply_chunk_start(req, EVHTP_RES_OK);
    errcode = btree_lookup(&rq, ctx->root_pointer);
    evhtp_send_reply_chunk_end(req);
}










int
main(int argc, char ** argv) {
    tree_file *file = (tree_file *) calloc(1, sizeof(*file));
    uint64_t root_pointer = 0;
    //open_view_file("/home/vmx/go/src/github.com/abhi-bit/gouch/example/pymc0_index", &file, &root_pointer);
    open_view_file("/tmp/pymc0_index", &file, &root_pointer);
    fprintf(stdout, "root pos %ld\n", root_pointer);

    request_ctx ctx = {file, root_pointer};


    evbase_t * evbase = event_base_new();
    evhtp_t  * htp    = evhtp_new(evbase, NULL);

    evhtp_set_cb(htp, "/simple/", testcb, "simple");
    evhtp_set_cb(htp, "/1/ping", testcb, "one");
    evhtp_set_cb(htp, "/1/ping.json", testcb, "two");
    evhtp_set_cb(htp, "/issue161", issue161cb, NULL);
    evhtp_set_cb(htp, "/chunked", chunked, NULL);
    evhtp_set_cb(htp, "/query", query, (void *)&ctx);
#ifndef EVHTP_DISABLE_EVTHR
    evhtp_use_threads(htp, NULL, 8, NULL);
#endif
    evhtp_bind_socket(htp, "0.0.0.0", 8081, 2048);

    event_base_loop(evbase, 0);

    evhtp_unbind_socket(htp);
    evhtp_free(htp);
    event_base_free(evbase);

    return 0;
}

