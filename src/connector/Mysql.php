<?php

namespace wxg\softdelete\connector;

use PDO;
use wxg\softdelete\Connection;
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

    /**
     * 解析pdo连接的dsn信息
     * @access protected
     * @param array $config 连接信息
     * @return string
     */
    protected function parseDsn($config)
    {
        return $this->tpConnection->parseDsn($config);
    }

    /**
     * 取得数据表的字段信息
     * @access public
     * @param string $tableName
     * @return array
     */
    public function getFields($tableName)
    {
        return $this->tpConnection->getFields($tableName);
    }

    /**
     * 取得数据库的表信息
     * @access public
     * @param string $dbName
     * @return array
     */
    public function getTables($dbName = '')
    {
        return $this->tpConnection->getTables($dbName);
    }

    /**
     * SQL性能分析
     * @access protected
     * @param string $sql
     * @return array
     */
    protected function getExplain($sql)
    {
        return $this->tpConnection->getExplain($sql);
    }
}