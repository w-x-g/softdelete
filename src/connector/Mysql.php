<?php

namespace wxg\softdelete\connector;

use think\db\connector\Mysql AS TPMysql;

//软删除数据库连接对象
class Mysql Extends Connection
{
    protected $builder = '\\think\\db\\builder\\Mysql';

    /**
     * 构造函数 读取数据库配置信息
     * @access public
     * @param array $config 数据库配置数组
     */
    public function __construct(array $config = [])
    {
        //调用父类构函数
        parent::__construct($config);
        
        //组合tp框架的Mysql类
        $this->tpConnection = new TPMysql($config);
    }
}