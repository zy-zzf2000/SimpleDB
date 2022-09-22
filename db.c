#include "db.h"
#include "apue.h"

#include <fcntl.h>		/* open & db_open flags */
#include <stdarg.h>
#include <errno.h>
#include <sys/uio.h>	/* struct iovec */

/*
 * Internal index file constants.
 * These are used to construct records in the
 * index file and data file.
 */
#define IDXLEN_SZ	   4	/* index record length (ASCII chars) */
#define SEP         ':'	/* separator char in index record */
#define SPACE       ' '	/* space character */
#define NEWLINE     '\n'	/* newline character */

/*
 * The following definitions are for hash chains and free
 * list chain in the index file.
 */

#define PTR_SZ        7	/* size of ptr field in hash chain */
#define PTR_MAX 9999999	/* max file offset = 10**PTR_SZ - 1 */
#define NHASH_DEF	 137	/* default hash table size */
#define FREE_OFF      0	/* free list offset in index file */
#define HASH_OFF PTR_SZ	/* hash table offset in index file */

typedef unsigned long	DBHASH;	/* hash values */
typedef unsigned long	COUNT;	/* unsigned counter */

//DB结构体
/*
    索引文件结构：
    | 空闲链表指针 | hash表（由NHASH_DEF个散列链表头指针构成） | 索引记录 | 索引记录 | ... |
    索引记录结构：
    | 链表指针（指向散列链表下一个元素） | 索引记录长度(占idxlen个字节) | key | 分隔符 | 数据指针 | 分隔符 | 数据记录长度 | \n |
*/
typedef struct{
    int idxfd;  //索引fd
    int datafd;  //文件fd

    char* idxbuf;  //索引缓冲区
    char* databuf; //数据缓冲区

    char* name;  //文件名

    off_t idxoff;  //索引的偏移量，等于一个指针的字节数（空闲链表指针）
    size_t idxlen;  //索引记录的长度

    off_t  datoff;  //存储查询到的数据记录的偏移量
    size_t datlen;  //存储查询到的数据记录的长度

    off_t  ptrval; /* contents of chain ptr in index record */
    off_t  ptroff; /* chain ptr offset pointing to this idx record */
    off_t  chainoff; /* offset of hash chain for this index record */
    off_t  hashoff;  /* offset in index file of hash table */
    DBHASH nhash;    /* current hash table size */
    COUNT  cnt_delok;    /* delete OK */
    COUNT  cnt_delerr;   /* delete error */
    COUNT  cnt_fetchok;  /* fetch OK */
    COUNT  cnt_fetcherr; /* fetch error */
    COUNT  cnt_nextrec;  /* nextrec */
    COUNT  cnt_stor1;    /* store: DB_INSERT, no empty, appended */
    COUNT  cnt_stor2;    /* store: DB_INSERT, found empty, reused */
    COUNT  cnt_stor3;    /* store: DB_REPLACE, diff len, appended */
    COUNT  cnt_stor4;    /* store: DB_REPLACE, same len, overwrote */
    COUNT  cnt_storerr;  /* store error */
}DB;

//内部函数

static DB     *_db_alloc(int);
static void    _db_dodelete(DB *);
static int	    _db_find_and_lock(DB *, const char *, int);
static int     _db_findfree(DB *, int, int);
static void    _db_free(DB *);
static DBHASH  _db_hash(DB *, const char *);
static char   *_db_readdat(DB *);
static off_t   _db_readidx(DB *, off_t);
static off_t   _db_readptr(DB *, off_t);
static void    _db_writedat(DB *, const char *, off_t, int);
static void    _db_writeidx(DB *, const char *, off_t, int, off_t);
static void    _db_writeptr(DB *, off_t, off_t);

//分配一个数据库所需的内存空间
static DB* _db_alloc(int namelen){
    DB* db;

    //分配DB结构体内存
    db = malloc(sizeof(DB));
    if(db==NULL) err_dump("db malloc error");

    //初始化DB索引和文件fd
    db->idxfd = -1;
    db->datafd = -1;

    //分配DB名称内存
    db->name = malloc(namelen+5);
    if(db->name==NULL) err_dump("db name malloc error");

    //分配读写缓冲区内存,数据可以先写入数据库的缓冲区，再写入内核缓冲区中
    db->idxbuf = malloc(IDXLEN_MAX+2);
    if(db->idxbuf==NULL) err_dump("db idxbuf malloc error");
    db->databuf = malloc(DATLEN_MAX+2);
    if(db->databuf==NULL) err_dump("db databuf malloc error");

    return db;
}