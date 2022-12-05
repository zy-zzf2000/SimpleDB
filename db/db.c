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

typedef unsigned long	DBHASH;	//根据key计算出的hash值
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

    char* idxbuf;  //索引缓冲区,用于暂时的存储读取到的索引记录
    char* databuf; //数据缓冲区，用于暂时的存储读取到的数据

    char* name;  //文件名

    off_t idxoff;  //第一条索引记录的偏移量，等于一个指针的字节数（空闲链表指针）
    size_t idxlen;  //索引记录的长度

    off_t  datoff;  //存储查询到的数据记录的偏移量
    size_t datlen;  //存储查询到的数据记录的长度

    off_t  ptrval; /* 索引文件中的指针内容 */
    off_t  ptroff;   //存储指向该索引的指针的偏移量
    off_t  chainoff; //存储当前查询key所在链表的头指针的偏移量
    off_t  hashoff;  //存储当前查询key对应的索引记录的偏移量
    DBHASH nhash;    //哈希表大小

    //cnt开头的COUNT类型变量用于记录各种操作的成功和失败次数(因此是可选的)
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

//从空闲链表中找到一个key size和data size均满足的空闲空间
static int  _db_findfree(DB *db, int keylen, int datlen){
    int rc;
    off_t offset, nextoffset, saveoffset;

    //首先对空闲链表加锁
    if(writew_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0) err_dump("_db_findfree: writew_lock error");

    //saveoffset存储空闲链表中的指针偏移量,可以看作是指针的指针，指针的地址
    saveoffset = FREE_OFF;

    //offset存储空闲链表中的指针内容，索引记录的地址
    offset = _db_readptr(db,saveoffset);

    //对于一个指针来说，saveoffset是它的地址，offset是它的内容

    //遍历空闲链表
    while(offset!=0){
        nextoffset = _db_readidx(db,offset); //读取空闲空间的下一个空闲空间的指针

        //如果空闲空间的key size和data size均满足要求，则返回空闲空间的偏移量
        if(strlen(db->idxbuf) == keylen && db->datlen == datlen) break;

        //否则，继续遍历空闲链表
        saveoffset = offset;
        offset = nextoffset;
    }

    //如果offset不为0，则说明找到一个符合条件的空闲块
    if(offset==0){
        rc = -1;
    }else{
        //当前找到的空间是
        _db_writeptr(db,saveoffset,db->ptrval);
        rc = 0;
    }

    //解锁空闲链表
    un_lock(db->idxfd,FREE_OFF,SEEK_SET,1);
    return (rc);

}

//从数据文件中,datoff偏移量处，读取datlen长度的数据到datbuf缓冲区
static char* _db_readdat(DB *db){
    lseek(db->datafd,db->datoff,SEEK_SET);
    read(db->datafd,db->databuf,db->datlen);
    if(db->databuf[db->datlen-1] != NEWLINE){
        err_dump("_db_readdat: missing newline");
    }else{
        db->databuf[db->datlen-1] = 0;
    }
    return db->databuf;
}

//读取对应偏移量的索引记录，将其存储在idxbuf中，并且返回索引链表下一条索引记录的偏移量
static off_t   _db_readidx(DB *db, off_t offset){

    //首先读取下一条索引记录的偏移量以及索引记录的长度(定长部分)
    char asciiptr[PTR_SZ+1];
    char recordlen[IDXLEN_SZ+1];
    struct iovec iov[2];
    if(lseek(db->idxfd,offset,offset==0?SEEK_CUR:SEEK_SET)==-1){
        err_dump("_db_readidx:lseek error");
    } 
    iov[0].iov_base = asciiptr;
    iov[0].iov_len = PTR_SZ;
    iov[1].iov_base = recordlen;
    iov[1].iov_len = IDXLEN_SZ;

    if(readv(db->idxfd,iov,2)!=PTR_SZ+IDXLEN_SZ){
        err_dump("_db_readidx:readv error");
    }

    asciiptr[PTR_SZ] = 0;
    recordlen[IDXLEN_SZ] = 0;
    
    //将下一条索引记录的偏移量存入ptrval
    db->ptrval = atol(asciiptr);
    
    db->idxlen = atol(recordlen);

    //根据idxlen，我们可以将索引记录的真正内容读取出来
    if(read(db->idxfd,db->idxbuf,db->idxlen)!=db->idxlen){
        err_dump("_db_readidx:read error");
    }

    if(db->idxbuf[db->idxlen-1]!=NEWLINE){
        err_dump("_db_readidx:missing newline");
    }else{
        db->idxbuf[db->idxlen-1] = 0;
    }

    //读取键、数据记录的偏移量以及数据记录的长度
    char *ptr1,*ptr2;
    if((ptr1 = strchr(db->idxbuf,SEP))==NULL){
        err_dump("_db_readidx:missing first separator");
    }
    *ptr1++ = 0;  //将第一个分隔符替换成0，方便直接读取

    if(ptr2 = strchr(ptr1,SEP)==NULL){
        err_dump("_db_readidx:missing second separator");
    }

    *ptr2++ = 0;

    //将两个分割符都替换成\0,方便直接读取，现在idxbuf中的内容是： key内容\0 + 数据记录偏移量\0 + 数据长度\0
    //想要得到key，直接对idxbuf进行read即可
    //想要得到数据记录的偏移量，对ptr1进行read即可
    //想要得到数据长度，对ptr2进行read即可

    db->datoff = atol(ptr1);
    db->datlen = atol(ptr2);

    return(db->ptrval);



}

//读取索引指针指的内容(注意不是指针指向的内容,这里只是将指针的偏移量读出来)
//读取指针的内容，将其放入ptrval中，并且返回当前指针的下一个指针地址
static off_t  _db_readptr(DB *db, off_t offset){
    char asciiptr[PTR_SZ+1];
    //首先将索引文件的文件偏移移动到offset指定位置
    if(lseek(db->idxfd,offset,SEEK_SET)==-1){
        err_dump("_db_readptr_:lseek error to ptr field");
    }
    if(read(db->idxfd,asciiptr,PTR_SZ)==-1){
        err_dump("_db_readptr_:read error");
    }
    asciiptr[PTR_SZ] = 0; //补上最后的\0
    return atol(asciiptr);
}

//根据键值计算hash值
static DBHASH  _db_hash(DB *db, const char *key){
    DBHASH hval = 0;
    char ch;
    int i;
    for(i=1;(ch = *key++)!=0;i++){
        hval += ch*i;
    }
    return hval % db->nhash;
}

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

void db_close(DBHANDLE h){
    _db_free(h);
}

//打开一个数据库，其参数与系统调用open相同
DBHANDLE db_open(const char* pathname,int flags,...){
    DB			*db;
	int			len, mode;
	size_t		i;
	char		asciiptr[PTR_SZ + 1],
				hash[(NHASH_DEF + 1) * PTR_SZ + 2];  /*散列表,每个元素占PTR_SZ个字节，最后两个字节用于存储换行符和空字符*/
					/* +2 for newline and null */
	struct stat	statbuff;

    len = strlen(pathname);

    //分配DB所需空间(这里不包括索引和数据文件)
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
        //创建数据库，我们需要取得第三个权限参数（varargs）
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
            //%*d表示输出的宽度为PTR_SZ，不足的用空格填充
            sprintf(asciiptr,"%*d",PTR_SZ,0);
            //初始化空闲链表指针和哈希表指针，在一个idx数据中，共有哈希表指针NHASH_DEF个，空闲链表指针1个，一共NHASH_DEF+1个指针，每个指针占PTR_SZ个字节
            hash[0] = 0; //先添加一个终止符，用于后面的strcat
            for(i=0;i<NHASH_DEF+1;i++){
                strcat(hash,asciiptr);
            }
            //指针区域和索引记录区域用一个换行符分隔
            strcat(hash,"\n");  //添加换行符

            //将hash写入索引fd
            if(write(db->idxfd,hash,strlen(hash))!=strlen(hash)) err_dump("db_open write error");
        }
        //完成对指针的初始化后，需要关闭锁
        if(un_lock(db->idxfd,0,SEEK_SET,0)<0) err_dump("db_open un_lock error");   //解锁同样是调用fcntl函数实现，cmd为F_SETLK，l_type为F_UNLCK
    }
    db_rewind(db);  //将索引文件指针指向第一个记录
    return(db);
}

//从指定的数据库中读取一条记录
char* db_fetch(DBHANDLE h, const char *key){
    DB* db;
    char* ptr;

    //调用_db_find_and_lock函数，对指定的key查找并且加锁
    int res = _db_find_and_lock(h,key,0);
    if(res<0){
        //没有找到指定记录
        ptr = NULL;
        db->cnt_fetcherr += 1;  
    }else{
        ptr = _db_readdat(db);
        db->cnt_fetchok += 1;
    }
    //解锁
    if(un_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0) err_dump("db_fetch un_lock error");
    return ptr;
}

static int _db_find_and_lock(DB *db, const char *key, int writelock){
    //首先找到这个key对应的hash table的位置
    off_t offset, nextoffset;

    //计算hash值
    db->chainoff = (_db_hash(db,key)*PTR_SZ)+db->hashoff;
    db->hashoff = db->chainoff;

    //对所在的链表加锁,这里采用细粒度的锁，即对某个hash链表的第一个字节加上记录锁，而不是整个文件加锁
    //同时，这里采用的是阻塞式的锁，如果不能获取到锁，则进程会一直阻塞
    if(writelock){
        if(writew_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0){
            err_dump("_db_find_and_lock:write_lock_error");
        }
    }else{
        if(readw_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0){
            err_dump("_db_find_and_lock:readw_lock_error");
        }
    }

    //开始遍历该哈希桶的链表，直到遍历到尾部，或者找到key为止
    offset = _db_readptr(db,db->ptroff);
    while(offset!=0){
        //读取offset指向的索引记录
        nextoffset = _db_readidx(db,offset);
        if(strcmp(db->idxbuf,key)==0) break; //找到了
        db->ptroff = offset;
        offset = nextoffset;
    }
    return offset==0?-1:0;
}

int db_store(DBHANDLE db, const char *key, const char *data, int flag){
    DB *h = (DB*)db;
    //首先判断flag是否有效
    if(flag!=DB_INSERT && flag!=DB_REPLACE && flag!=DB_STORE){
        errno = EINVAL;
        return -1;
    }

    int keylen = strlen(key);
    int datlen = strlen(data)+1;    //+1是为了存储换行符
    if(datlen<DATLEN_MIN || datlen>DATLEN_MAX){
        err_dump("db_store:invalid data length");
    }

    //检查key是否已经存在
    if(_db_find_and_lock(db,key,1)==-1){
        //不存在
        if(flag==DB_REPLACE){
            //如果是替换，则返回错误
            if(un_lock(h->idxfd,h->chainoff,SEEK_SET,1)<0) err_dump("db_store un_lock error");
            errno = ENOENT;
            return -1;
        }else{

        }
    }else{
        //存在
    }
}