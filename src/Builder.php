<?php

namespace wxg\softdelete;

use think\db\connector\Mysql AS TPMysql;

//软删除数据库驱动
class Mysql Extends TPMysql
{
    protected $builder = 'Mysql';
}