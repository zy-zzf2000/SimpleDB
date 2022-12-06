/*
 * @Author: zy 953725892@qq.com
 * @Date: 2022-12-05 11:30:11
 * @LastEditors: zy 953725892@qq.com
 * @LastEditTime: 2022-12-06 14:42:37
 * @FilePath: /db/main.c
 * @Description: 
 * 
 * Copyright (c) 2022 by zy 953725892@qq.com, All Rights Reserved. 
 */
#include <stdio.h>
#include "db.h"
#include "apue.h"

// #include <sys/types.h>
// #include <sys/stat.h>
#include <fcntl.h>

int main(){
    DBHANDLE db;
    db = db_open("db", O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    db_store(db,"test","test",DB_INSERT);
    char* value = db_fetch(db,"test");
    printf("%s",value);
    return 0;
}