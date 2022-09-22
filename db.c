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
    | 空闲链表指针 | hash表（由NHASH_DEF个散列链表头指针构成） | \n | 索引记录 | 索引记录 | ... |
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

static void _db_free(DB *db){
    if (db->idxfd >= 0)
		close(db->idxfd);
	if (db->datafd >= 0)
		close(db->datafd);
	if (db->idxbuf != NULL)
		free(db->idxbuf);
	if (db->databuf != NULL)
		free(db->databuf);
	if (db->name != NULL)
		free(db->name);
	free(db);
}

//打开一个数据库，其参数与系统调用open相同
static DBHANDLE db_open(const char* pathname,int flags,...){
    DB			*db;
	int			len, mode;
	size_t		i;
	char		asciiptr[PTR_SZ + 1],
				hash[(NHASH_DEF + 1) * PTR_SZ + 2];  /*散列表,每个元素占PTR_SZ个字节，最后两个字节用于存储换行符和空字符*/
					/* +2 for newline and null */
	struct stat	statbuff;

    len = strlen(pathname);

    //分配DB所需空间
    db = _db_alloc(strlen(pathname));
    if(db==NULL) err_dump("db_open malloc error");

    //分配db的哈希表结构
    db->nhash = NHASH_DEF;
    db->hashoff = HASH_OFF;

    //分配db名称
    db->name = strcpy(db->name,pathname);
    db->name = strcat(db->name,".idx");  //准备用于创建索引文件

    //创建数据库
    if(flags & O_CREAT){
        //创建数据库，我们需要取得第三个权限参数
        va_list ap;
        va_start(ap,flags);
        mode = va_arg(ap,mode_t);
        va_end(ap);

        //创建索引和数据文件
        db->idxfd = open(db->name,flags,mode);
        strcpy(db->name+len,".dat");  //strcpy的作用是将dst拷贝(如果src原本有内容则覆盖)到src指针所指向的位置
        db->datafd = open(db->name,flags,mode);
    }
    else{
        //否则就是正常的打开数据库
        db->idxfd = open(db->name,flags);
        db->name = strcpy(db->name+len,".dat");
        db->datafd = open(db->name,flags);
    }

    //fd打开失败
    if(db->datafd<0 || db->idxfd<0){
        _db_free(db);
        return NULL;
    }

    //如果是创建新的数据库，或者对原本的数据库进行格式化，那么我们必须要对数据库的索引文件指针进行初始化操作
    if(flags & (O_CREAT | O_TRUNC)==(O_CREAT|O_TRUNC)){
        //初始化时，必须对idx文件进行加锁，防止丢失其他进程对数据库的修改
        if(writew_lock(db->idxfd,0,SEEK_SET,0)<0) err_dump("db_open writew_lock error");   //加锁需要调用fcntl函数，参考书中392 fcntl(int fd,int cmd(F_SETLK),flock*)
        //其中flock* 主要包含 l_type(锁类型) l_whence(偏移量) l_start(起始位置) l_len(加锁长度) l_pid(进程id，无需填写，用于F_GETLK cmd的返回值)

        //查看索引文件的状态
        struct stat* statbuff;
        if(fstat(db->idxfd,statbuff)<0) err_dump("db_open fstat error");

        if(statbuff->st_size==0){
            //首先创建一个0的ASCII编码，在本项目中，所有指针都用ASCII偏移量来表示，而0代表了空指针
            sprintf(asciiptr,"%*d",PTR_SZ,0);
            //初始化空闲链表指针和哈希表指针，在一个idx数据中，共有哈希表指针NHASH_DEF个，空闲链表指针1个，一共NHASH_DEF+1个指针，每个指针占PTR_SZ个字节
            hash[0] = 0; //先添加一个终止符，用于后面的strcat
            for(i=0;i<NHASH_DEF+1;i++){
                strcat(hash,asciiptr);
            }
            stract(hash,"\n");  //添加换行符

            //将hash写入索引fd
            if(write(db->idxfd,hash,strlen(hash))!=strlen(hash)) err_dump("db_open write error");
        }
        //完成对指针的初始化后，需要关闭锁
        if(un_lock(db->idxfd,0,SEEK_SET,0)<0) err_dump("db_open un_lock error");   //解锁同样是调用fcntl函数实现，cmd为F_SETLK，l_type为F_UNLCK
    }
    db_rewind(db);  //将索引文件指针指向第一个记录
    return(db);
}