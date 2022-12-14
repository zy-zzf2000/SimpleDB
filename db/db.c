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

    off_t  ptrval;   //索引文件中的指针内容 
    off_t  ptroff;   //存储指向该索引的指针的偏移量
    off_t  chainoff; //存储当前查询key所在链表的头指针的偏移量
    off_t  hashoff;  //存储第一个哈希桶的偏移量

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


//将idx的文件偏移量移动到索引记录的起始位置(即空闲链表+哈希表字节偏移之后)
void db_rewind(DBHANDLE h){
    DB		*db = h;
	off_t	offset;

	offset = (db->nhash + 1) * PTR_SZ;	/* +1 for free list ptr */

	/*
	 * We're just setting the file offset for this process
	 * to the start of the index records; no need to lock.
	 * +1 below for newline at end of hash table.
	 */
	if ((db->idxoff = lseek(db->idxfd, offset+1, SEEK_SET)) == -1)
		err_dump("db_rewind: lseek error");
}

//删除当前db所指向的记录；
//将其数据和索引文件的键清空为空白
//更新其所在的哈希桶
//将该节点的空间加入到空闲链表中去
static void _db_dodelete(DB *db){
    int		i;
	char	*ptr;
	off_t	freeptr, saveptr;

    //写一个长度与待删除记录的数据一致的空白数据
    for (ptr = db->databuf, i = 0; i < db->datlen - 1; i++) *ptr++ = SPACE;
    *ptr = 0;

    //写一个长度与待删除记录的索引
    ptr = db->idxbuf;
	while (*ptr) *ptr++ = SPACE;

    //开始进行删除操作
    //对freelist加锁
    writew_lock(db->idxfd, FREE_OFF, SEEK_SET, 1);
    //清空数据
    _db_writedat(db, db->databuf, db->datoff, SEEK_SET);


    //读取freelist的头指针
    freeptr = _db_readptr(db, FREE_OFF);

    //ptrval记录了当前索引记录的指针内容，也就是当前索引记录下一条索引记录的偏移量
    saveptr = db->ptrval;

    //清空索引记录，并且让索引记录的指针指向freelist的头指针
    _db_writeidx(db,db->idxbuf,db->idxoff,SEEK_SET,freeptr);

    //更新freelist的头指针为当前删除的节点
    _db_writeptr(db,FREE_OFF,db->idxoff);

    //更新删除节点所在哈希链表，将ptroff指向的指针指向ptrval
    _db_writeptr(db,db->ptroff,saveptr);

    //解锁freelist
    un_lock(db->idxfd,FREE_OFF,SEEK_SET,1);

}

static void _db_writedat(DB *db, const char *data, off_t offset, int whence)
{
	struct iovec	iov[2];
	static char		newline = NEWLINE;

	//与写入索引文件一样，如果是追加写入，则需要保证lseek和write是原子操作（否则如果有两个进程同时追加，会导致数据错乱）
    //如果是覆盖写入，则不需要保证原子性,因为findfree函数保证了每个空闲块最多只有一个进程使用，因此不会出现多个进程同时覆盖写入同一个位置的情况
	if (whence == SEEK_END) /* we're appending, lock entire file */
		if (writew_lock(db->datafd, 0, SEEK_SET, 0) < 0)
			err_dump("_db_writedat: writew_lock error"); 

	if ((db->datoff = lseek(db->datafd, offset, whence)) == -1)
		err_dump("_db_writedat: lseek error");
	db->datlen = strlen(data) + 1;	/* datlen includes newline */

	iov[0].iov_base = (char *) data;
	iov[0].iov_len  = db->datlen - 1;
	iov[1].iov_base = &newline;
	iov[1].iov_len  = 1;
	if (writev(db->datafd, &iov[0], 2) != db->datlen)
		err_dump("_db_writedat: writev error of data record");

	if (whence == SEEK_END)
		if (un_lock(db->datafd, 0, SEEK_SET, 0) < 0)
			err_dump("_db_writedat: un_lock error");
}

//向idx文件的offset(和whence)处写入一条索引记录，该记录的键为key，下一条索引记录的偏移量为ptrval，dat的偏移量为datoff，dat的长度为datlen
static void _db_writeidx(DB *db, const char *key,
             off_t offset, int whence, off_t ptrval)
{
	struct iovec	iov[2];
	char			asciiptrlen[PTR_SZ + IDXLEN_SZ + 1];
	int				len;

	if ((db->ptrval = ptrval) < 0 || ptrval > PTR_MAX)
		err_quit("_db_writeidx: invalid ptr: %d", ptrval);
	sprintf(db->idxbuf, "%s%c%lld%c%ld\n", key, SEP,   //%lld是long long int的格式化输出,%ld是long int的格式化输出
	  (long long)db->datoff, SEP, (long)db->datlen);
	len = strlen(db->idxbuf);
	if (len < IDXLEN_MIN || len > IDXLEN_MAX)
		err_dump("_db_writeidx: invalid length");
	sprintf(asciiptrlen, "%*lld%*d", PTR_SZ, (long long)ptrval,
	  IDXLEN_SZ, len);

    //如果是追加，那么lseek和write必须对整个文件加锁，否则会出现多个进程同时写入同一文件的情况
    //如果不是追加，那么无需加锁
	if (whence == SEEK_END)		/* we're appending */
		if (writew_lock(db->idxfd, ((db->nhash+1)*PTR_SZ)+1,
		  SEEK_SET, 0) < 0)
			err_dump("_db_writeidx: writew_lock error");

	//这里用lseek来获取当前文件的偏移量
	if ((db->idxoff = lseek(db->idxfd, offset, whence)) == -1)
		err_dump("_db_writeidx: lseek error");

	iov[0].iov_base = asciiptrlen;
	iov[0].iov_len  = PTR_SZ + IDXLEN_SZ;
	iov[1].iov_base = db->idxbuf;
	iov[1].iov_len  = len;
	if (writev(db->idxfd, &iov[0], 2) != PTR_SZ + IDXLEN_SZ + len)
		err_dump("_db_writeidx: writev error of index record");

	if (whence == SEEK_END)
		if (un_lock(db->idxfd, ((db->nhash+1)*PTR_SZ)+1,
		  SEEK_SET, 0) < 0)
			err_dump("_db_writeidx: un_lock error");
}

//将一个ptrval值写入索引文件的ptrval指针处
static void _db_writeptr(DB *db, off_t offset, off_t ptrval)
{
	char	asciiptr[PTR_SZ + 1];

	if (ptrval < 0 || ptrval > PTR_MAX)
		err_quit("_db_writeptr: invalid ptr: %d", ptrval);
	sprintf(asciiptr, "%*lld", PTR_SZ, (long long)ptrval);

	if (lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("_db_writeptr: lseek error to ptr field");
	if (write(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("_db_writeptr: write error of ptr field");
}

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
        nextoffset = _db_readidx(db,offset); //读取offset处的索引记录，同时得到下一个索引记录的地址
        //注意在_db_readidx中，offset处索引记录记录的包括idxoff,datoff在内的信息会被存储到db中

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
        //当前找到的空间是offset指向的空间，指向这个空间的指针存储在saveoffset中
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
//同时还会将数据记录的偏移量存储在datoff中，数据记录的长度存储在datlen中
//将索引记录的偏移量记录在idxoff中
//填充的内容包括：idxbuf,datoff,datlen,idxoff
//offset是这条索引记录在idx文件中的偏移量
static off_t   _db_readidx(DB *db, off_t offset){

    //首先读取下一条索引记录的偏移量以及索引记录的长度(定长部分)
    char asciiptr[PTR_SZ+1];
    char recordlen[IDXLEN_SZ+1];
    struct iovec iov[2];
    if((db->idxoff = lseek(db->idxfd,offset,offset==0?SEEK_CUR:SEEK_SET))==-1){
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

    if((ptr2 = strchr(ptr1,SEP))==NULL){
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

    db->cnt_delerr = 0;
    db->cnt_fetcherr = 0;
    db->cnt_nextrec = 0;
    db->cnt_stor1 = 0;
    db->cnt_stor2 = 0;
    db->cnt_stor3 = 0;
    db->cnt_stor4 = 0;
    db->cnt_storerr = 0;


    db_rewind(db);  //将索引文件指针指向第一个记录
    return(db);
}

//从指定的数据库中读取一条记录
char* db_fetch(DBHANDLE h, const char *key){
    DB *db = (DB*) h;
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
    db->ptroff = db->chainoff;

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
    int rc;
    off_t ptrval;
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
    //这里会保存key对应的哈希桶的偏移量
    if(_db_find_and_lock(h,key,1)==-1){
        //不存在
        if(flag==DB_REPLACE){
            //如果是替换，则返回错误
            if(un_lock(h->idxfd,h->chainoff,SEEK_SET,1)<0) err_dump("db_store un_lock error");
            errno = ENOENT;
            return -1;
        }else{
            //否则是插入，需要将key和data写入索引文件和数据文件
            //ptrval中存储了需要插入的数据所在哈希桶第一条记录的偏移量，它会被作为插入数据的next指针
            //可以看出，插入使用的是头插法
            ptrval = _db_readptr(h,h->chainoff);
            //首先尝试是否能够重用空闲链表
            if(_db_findfree(h,keylen,datlen)<0){      
                //不能重用，需要将数据追加到数据文件和索引文件的尾部

                //注意三个write的顺序不能颠倒，在writedat中会首先向dat文件追加数据，然后将数据的长度和偏移量保存在datlen和datoffset中
                //之后再writeidx中会将索引记录的偏移量保存在idxoff中
                _db_writedat(h,data,h->datoff,SEEK_END);
                _db_writeidx(h,key,h->idxoff,SEEK_END,ptrval); //头插法，将新的索引记录插入到链表的头部，原本的第一条记录的偏移量作为新记录的next指针
                _db_writeptr(h,h->chainoff,h->idxoff);       //头插法,将哈希桶的头指针指向新插入的索引记录
                h->cnt_stor1++;
                return 1;
            }else{
                //可以重用，此时直接将内容写入findfree中找到的idxoff和datoff
                _db_writedat(h, data, h->datoff, SEEK_SET);
                _db_writeidx(h, key, h->idxoff, SEEK_SET, ptrval);
                _db_writeptr(h, h->chainoff, h->idxoff);
                h->cnt_stor2++;
                return 1;
            }
        }
    }else{
        //存在
        if(flag==DB_INSERT){
            //如果是插入，则返回错误
            if(un_lock(h->idxfd,h->chainoff,SEEK_SET,1)<0) err_dump("db_store un_lock error");
            errno = EEXIST;
            return -1;
        }else{
            //否则是替换，需要将数据写入数据文件
            if(datlen==h->datlen){
                //如果长度一致，那么直接覆盖
                _db_writedat(h,data,h->datoff,SEEK_SET);
                h->cnt_stor3++;
                return 1;
            }else{
                //如果长度不一致，那么需要将数据追加到数据文件的尾部
                _db_dodelete(h);	
                ptrval = _db_readptr(h, h->chainoff);
                _db_writedat(h, data, 0, SEEK_END);
                _db_writeidx(h, key, 0, SEEK_END, ptrval);
                _db_writeptr(h, h->chainoff, h->idxoff);
                h->cnt_stor4++;
                return 1;
            }
        }
    }
}