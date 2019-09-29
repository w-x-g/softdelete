<?php
namespace wxg\softdelete;

use PDO;
use think\db\Query as TPQuery;

//软删除查询对象
class Query extends TPQuery
{
    /**
     * 得到某个字段的值
     * @access public
     * @param string $field   字段名
     * @param mixed  $default 默认值
     * @param bool   $force   强制转为数字类型
     * @param bool   $softdelete   是否查询软删除的数据，默认否
     * @return mixed
     */
    public function value($field, $default = null, $force = false, $softdelete = null)
    {
        $result = false;
        if (empty($this->options['table'])) {
            $this->options['table'] = $this->getTable();
        }
        if (empty($this->options['fetch_sql']) && !empty($this->options['cache'])) {
            // 判断查询缓存
            $cache  = $this->options['cache'];
            $key    = is_string($cache['key']) ? $cache['key'] : md5($field . serialize($this->options) . serialize($this->bind));
            $result = Cache::get($key);
        }
        if (false === $result) {
            if ($softdelete === null) {
                $this->options = $this->changeOptions($this->options);
            }
            $pdo = $this->field($field)->limit(1)->getPdo();
            $this->unsetField($this->options);
            if (is_string($pdo)) {
                // 返回SQL语句
                return $pdo;
            }
            $result = $pdo->fetchColumn();
            if ($force) {
                $result += 0;
            }

            if (isset($cache)) {
                // 缓存数据
                $this->cacheData($key, $result, $cache);
            }
        } else {
            // 清空查询条件
            // $this->options = [];
            $this->unsetField($this->options);
        }
        return false !== $result ? $result : $default;
    }

    /**
     * 得到某个列的数组
     * @access public
     * @param string $field 字段名 多个字段用逗号分隔
     * @param string $key   索引
     * @param bool   $softdelete 是否查询软删除的数据，默认否
     * @return array
     */
    public function column($field, $key = '', $softdelete = null)
    {
        $result = false;
        if (empty($this->options['table'])) {
            $this->options['table'] = $this->getTable();
        }
        if (empty($this->options['fetch_sql']) && !empty($this->options['cache'])) {
            // 判断查询缓存
            $cache  = $this->options['cache'];
            $guid   = is_string($cache['key']) ? $cache['key'] : md5($field . serialize($this->options) . serialize($this->bind));
            $result = Cache::get($guid);
        }
        if (false === $result) {
            if ($softdelete === null) {
                $this->options = $this->changeOptions($this->options);
            }
            if (is_null($field)) {
                $field = '*';
            } elseif ($key && '*' != $field) {
                $field = $key . ',' . $field;
            }
            $pdo = $this->field($field)->getPdo();
            $this->unsetField($this->options);
            if (is_string($pdo)) {
                // 返回SQL语句
                return $pdo;
            }
            if (1 == $pdo->columnCount()) {
                $result = $pdo->fetchAll(PDO::FETCH_COLUMN);
            } else {
                $resultSet = $pdo->fetchAll(PDO::FETCH_ASSOC);
                if ($resultSet) {
                    $fields = array_keys($resultSet[0]);
                    $count  = count($fields);
                    $key1   = array_shift($fields);
                    $key2   = $fields ? array_shift($fields) : '';
                    $key    = $key ?: $key1;
                    if (strpos($key, '.')) {
                        list($alias, $key) = explode('.', $key);
                    }
                    foreach ($resultSet as $val) {
                        if ($count > 2) {
                            $result[$val[$key]] = $val;
                        } elseif (2 == $count) {
                            $result[$val[$key]] = $val[$key2];
                        } elseif (1 == $count) {
                            $result[$val[$key]] = $val[$key1];
                        }
                    }
                } else {
                    $result = [];
                }
            }
            if (isset($cache) && isset($guid)) {
                // 缓存数据
                $this->cacheData($guid, $result, $cache);
            }
        } else {
            // 清空查询条件
            // $this->options = [];
            $this->unsetField($this->options);
        }
        return $result;
    }

    /**
     * COUNT查询
     * @access public
     * @param string $field 字段名
     * @param bool   $softdelete 是否查询软删除的数据，默认否
     * @return integer|string
     */
    public function count($field = '*', $softdelete = null)
    {
        if (isset($this->options['group'])) {
            // 支持GROUP
            $options = $this->getOptions();
            $subSql  = $this->options($options)->field('count(' . $field . ')')->bind($this->bind)->buildSql();
            return $this->table([$subSql => '_group_count_'])->value('COUNT(*) AS tp_count', 0, true, $softdelete);
        }

        return $this->value('COUNT(' . $field . ') AS tp_count', 0, true, $softdelete);
    }

    /**
     * SUM查询
     * @access public
     * @param string $field 字段名
     * @param bool   $softdelete 是否查询软删除的数据，默认否
     * @return float|int
     */
    public function sum($field, $softdelete = null)
    {
        return $this->value('SUM(' . $field . ') AS tp_sum', 0, true, $softdelete);
    }

    /**
     * MIN查询
     * @access public
     * @param string $field 字段名
     * @param bool   $force   强制转为数字类型
     * @param bool   $softdelete 是否查询软删除的数据，默认否
     * @return mixed
     */
    public function min($field, $force = true, $softdelete = null)
    {
        return $this->value('MIN(' . $field . ') AS tp_min', 0, $force, $softdelete);
    }

    /**
     * MAX查询
     * @access public
     * @param string $field 字段名
     * @param bool   $force   强制转为数字类型
     * @param bool   $softdelete 是否查询软删除的数据，默认否
     * @return mixed
     */
    public function max($field, $force = true, $softdelete = null)
    {
        return $this->value('MAX(' . $field . ') AS tp_max', 0, $force, $softdelete);
    }

    /**
     * AVG查询
     * @access public
     * @param string $field 字段名
     * @param bool   $softdelete 是否查询软删除的数据，默认否
     * @return float|int
     */
    public function avg($field, $softdelete = null)
    {
        return $this->value('AVG(' . $field . ') AS tp_avg', 0, true, $softdelete);
    }

    /**
     * 字段值(延迟)增长
     * @access public
     * @param string  $field    字段名
     * @param integer $step     增长值
     * @param integer $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setInc($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition) . serialize($this->bind));
            $step = $this->lazyWrite('inc', $guid, $step, $lazyTime);
            if (false === $step) {
                // 清空查询条件
                // $this->options = [];
                $this->unsetField($this->options);
                return true;
            }
        }
        return $this->setField($field, ['inc', $field, $step]);
    }

    /**
     * 字段值（延迟）减少
     * @access public
     * @param string  $field    字段名
     * @param integer $step     减少值
     * @param integer $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setDec($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition) . serialize($this->bind));
            $step = $this->lazyWrite('dec', $guid, $step, $lazyTime);
            if (false === $step) {
                // 清空查询条件
                // $this->options = [];
                $this->unsetField($this->options);
                return true;
            }
            return $this->setField($field, ['inc', $field, $step]);
        }
        return $this->setField($field, ['dec', $field, $step]);
    }

    /**
     * 去除查询参数
     * @access public
     * @param string|bool $option 参数名 true 表示去除所有参数
     * @return $this
     */
    public function removeOption($option = true)
    {
        if (true === $option) {
            // $this->options = [];
            $this->unsetField($this->options);
        } elseif (is_string($option) && isset($this->options[$option])) {
            unset($this->options[$option]);
        }
        return $this;
    }

    /**
     * 查找记录
     * @access public
     * @param array|string|Query|\Closure $data
     * @return Collection|false|\PDOStatement|string
     * @throws DbException
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     */
    public function select($data = null)
    {
        if ($data instanceof Query) {
            return $data->select();
        } elseif ($data instanceof \Closure) {
            call_user_func_array($data, [ & $me]);
            $data = null;
        }
        // 分析查询表达式
        $options = $this->parseExpress();

        if (false === $data) {
            // 用于子查询 不查询只返回SQL
            $options['fetch_sql'] = true;
        } elseif (!is_null($data) && $data !== true) {
            // 主键条件分析
            $this->parsePkWhere($data, $options);
        }

        $resultSet = false;
        if (empty($options['fetch_sql']) && !empty($options['cache'])) {
            // 判断查询缓存
            $cache = $options['cache'];
            unset($options['cache']);
            $key       = is_string($cache['key']) ? $cache['key'] : md5(serialize($options) . serialize($this->bind));
            $resultSet = Cache::get($key);
        }
        if (false === $resultSet) {
            // 生成查询SQL
            if ($data === null) {
                $options = $this->changeOptions($options);
            }
            $sql = $this->builder->select($options);
            // 获取参数绑定
            $bind = $this->getBind();
            if ($options['fetch_sql']) {
                // 获取实际执行的SQL语句
                return $this->connection->getRealSql($sql, $bind);
            }

            $options['data'] = $data;
            if ($resultSet = $this->trigger('before_select', $options)) {
            } else {
                // 执行查询操作
                $resultSet = $this->query($sql, $bind, $options['master'], $options['fetch_pdo']);

                if ($resultSet instanceof \PDOStatement) {
                    // 返回PDOStatement对象
                    return $resultSet;
                }
            }

            if (isset($cache) && false !== $resultSet) {
                // 缓存数据集
                $this->cacheData($key, $resultSet, $cache);
            }
        }

        // 数据列表读取后的处理
        if (!empty($this->model)) {
            // 生成模型对象
            $modelName = $this->model;
            if (count($resultSet) > 0) {
                foreach ($resultSet as $key => $result) {
                    /** @var Model $model */
                    $model = new $modelName($result);
                    $model->isUpdate(true);

                    // 关联查询
                    if (!empty($options['relation'])) {
                        $model->relationQuery($options['relation']);
                    }
                    // 关联统计
                    if (!empty($options['with_count'])) {
                        $model->relationCount($model, $options['with_count']);
                    }
                    $resultSet[$key] = $model;
                }
                if (!empty($options['with'])) {
                    // 预载入
                    $model->eagerlyResultSet($resultSet, $options['with']);
                }
                // 模型数据集转换
                $resultSet = $model->toCollection($resultSet);
            } else {
                $resultSet = (new $modelName)->toCollection($resultSet);
            }
        } elseif ('collection' == $this->connection->getConfig('resultset_type')) {
            // 返回Collection对象
            $resultSet = new Collection($resultSet);
        }
        // 返回结果处理
        if (!empty($options['fail']) && count($resultSet) == 0) {
            $this->throwNotFound($options);
        }
        return $resultSet;
    }

    /**
     * 查找单条记录
     * @access public
     * @param array|string|Query|\Closure $data
     * @return array|false|\PDOStatement|string|Model
     * @throws DbException
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     */
    public function find($data = null)
    {
        if ($data instanceof Query) {
            return $data->find();
        } elseif ($data instanceof \Closure) {
            call_user_func_array($data, [ & $this]);
            $data = null;
        }
        // 分析查询表达式
        $options = $this->parseExpress();
        $pk      = $this->getPk($options);
        if (!is_null($data) && $data !== true) {
            // AR模式分析主键条件
            $this->parsePkWhere($data, $options);
        } elseif (!empty($options['cache']) && true === $options['cache']['key'] && is_string($pk) && isset($options['where']['AND'][$pk])) {
            $key = $this->getCacheKey($options['where']['AND'][$pk], $options, $this->bind);
        }

        $options['limit'] = 1;
        $result           = false;
        if (empty($options['fetch_sql']) && !empty($options['cache'])) {
            // 判断查询缓存
            $cache = $options['cache'];
            if (true === $cache['key'] && !is_null($data) && !is_array($data)) {
                $key = 'think:' . (is_array($options['table']) ? key($options['table']) : $options['table']) . '|' . $data;
            } elseif (is_string($cache['key'])) {
                $key = $cache['key'];
            } elseif (!isset($key)) {
                $key = md5(serialize($options) . serialize($this->bind));
            }
            $result = Cache::get($key);
        }
        if (false === $result) {
            // 生成查询SQL
            if ($data === null) {
                $options = $this->changeOptions($options);
            }
            $sql = $this->builder->select($options);
            // 获取参数绑定
            $bind = $this->getBind();
            if ($options['fetch_sql']) {
                // 获取实际执行的SQL语句
                return $this->connection->getRealSql($sql, $bind);
            }
            if (is_string($pk)) {
                if (!is_array($data)) {
                    if (isset($key) && strpos($key, '|')) {
                        list($a, $val) = explode('|', $key);
                        $item[$pk]     = $val;
                    } else {
                        $item[$pk] = $data;
                    }
                    $data = $item;
                }
            }
            $options['data'] = $data;
            // 事件回调
            if ($result = $this->trigger('before_find', $options)) {
            } else {
                // 执行查询
                $resultSet = $this->query($sql, $bind, $options['master'], $options['fetch_pdo']);

                if ($resultSet instanceof \PDOStatement) {
                    // 返回PDOStatement对象
                    return $resultSet;
                }
                $result = isset($resultSet[0]) ? $resultSet[0] : null;
            }

            if (isset($cache) && false !== $result) {
                // 缓存数据
                $this->cacheData($key, $result, $cache);
            }
        }

        // 数据处理
        if (!empty($result)) {
            if (!empty($this->model)) {
                // 返回模型对象
                $model  = $this->model;
                $result = new $model($result);
                $result->isUpdate(true, isset($options['where']['AND']) ? $options['where']['AND'] : null);
                // 关联查询
                if (!empty($options['relation'])) {
                    $result->relationQuery($options['relation']);
                }
                // 预载入查询
                if (!empty($options['with'])) {
                    $result->eagerlyResult($result, $options['with']);
                }
                // 关联统计
                if (!empty($options['with_count'])) {
                    $result->relationCount($result, $options['with_count']);
                }
            }
        } elseif (!empty($options['fail'])) {
            $this->throwNotFound($options);
        }
        return $result;
    }

    /**
     * 删除记录
     * @access public
     * @param mixed $data 表达式 true 表示强制删除
     * @return int
     * @throws Exception
     * @throws PDOException
     */
    public function delete($data = null)
    {
        // 分析查询表达式
        $options = $this->parseExpress();
        $pk      = $this->getPk($options);
        if (isset($options['cache']) && is_string($options['cache']['key'])) {
            $key = $options['cache']['key'];
        }

        if (!is_null($data) && true !== $data) {
            if (!isset($key) && !is_array($data)) {
                // 缓存标识
                $key = 'think:' . $options['table'] . '|' . $data;
            }
            // AR模式分析主键条件
            $this->parsePkWhere($data, $options);
        } elseif (!isset($key) && is_string($pk) && isset($options['where']['AND'][$pk])) {
            $key = $this->getCacheKey($options['where']['AND'][$pk], $options, $this->bind);
        }

        if (true !== $data && empty($options['where'])) {
            // 如果条件为空 不进行删除操作 除非设置 1=1
            throw new Exception('delete without condition');
        }
        // 生成删除SQL语句，$data === null软删除，否者直接删除
        if ($data === null) {
            if ($this->hasForeignKeyRestrictData($options)) {
                throw new \Exception('删除失败，请先删除与该数据相关联的数据！');
            }
            $sql = $this->builder->update(array('delete_time' => date('Y-m-d H:i:s')), $options);
        } else {
            $sql = $this->builder->delete($options);
        }
        // 获取参数绑定
        $bind = $this->getBind();
        if ($options['fetch_sql']) {
            // 获取实际执行的SQL语句
            return $this->connection->getRealSql($sql, $bind);
        }

        // 检测缓存
        if (isset($key) && Cache::get($key)) {
            // 删除缓存
            Cache::rm($key);
        } elseif (!empty($options['cache']['tag'])) {
            Cache::clear($options['cache']['tag']);
        }
        // 执行操作
        $result = $this->execute($sql, $bind);
        if ($result) {
            if (!is_array($data) && is_string($pk) && isset($key) && strpos($key, '|')) {
                list($a, $val) = explode('|', $key);
                $item[$pk]     = $val;
                $data          = $item;
            }
            $options['data'] = $data;
            $this->trigger('after_delete', $options);
        }
        return $result;
    }

    /**
     * 软删除检验外键约束
     * @author Ultraman/2018-10-23
     * @return boolean
     */
    private function hasForeignKeyRestrictData($options)
    {
        $sql  = $this->builder->select($options);
        $bind = $this->getBind();
        //要删除的数据
        $resultSet = $this->query($sql, $bind, $options['master'], $options['fetch_pdo']);
        //外键约束关联表
        $schema = db('information_schema.key_column_usage')->where(array('CONSTRAINT_SCHEMA' => config('database.database'), 'REFERENCED_TABLE_NAME' => $options['table'], 'TABLE_NAME' => array('NEQ', 'logs')))->select(true);
        foreach ($schema as $k => $v) {
            foreach ($resultSet as $kk => $vv) {
                $has = db($v['TABLE_NAME'])->where(array($v['COLUMN_NAME'] => $vv[$v['REFERENCED_COLUMN_NAME']]))->count();
                if ($has) {
                    return $v['TABLE_NAME'];
                }
            }
        }
        return false;
    }

    /**
     * 分析表达式（可用于查询或者写入操作）
     * @access protected
     * @return array
     */
    protected function parseExpress()
    {
        $options = $this->options;

        // 获取数据表
        if (empty($options['table'])) {
            $options['table'] = $this->getTable();
        }

        if (!isset($options['where'])) {
            $options['where'] = [];
        } elseif (isset($options['view'])) {
            // 视图查询条件处理
            foreach (['AND', 'OR'] as $logic) {
                if (isset($options['where'][$logic])) {
                    foreach ($options['where'][$logic] as $key => $val) {
                        if (array_key_exists($key, $options['map'])) {
                            $options['where'][$logic][$options['map'][$key]] = $val;
                            unset($options['where'][$logic][$key]);
                        }
                    }
                }
            }

            if (isset($options['order'])) {
                // 视图查询排序处理
                if (is_string($options['order'])) {
                    $options['order'] = explode(',', $options['order']);
                }
                foreach ($options['order'] as $key => $val) {
                    if (is_numeric($key)) {
                        if (strpos($val, ' ')) {
                            list($field, $sort) = explode(' ', $val);
                            if (array_key_exists($field, $options['map'])) {
                                $options['order'][$options['map'][$field]] = $sort;
                                unset($options['order'][$key]);
                            }
                        } elseif (array_key_exists($val, $options['map'])) {
                            $options['order'][$options['map'][$val]] = 'asc';
                            unset($options['order'][$key]);
                        }
                    } elseif (array_key_exists($key, $options['map'])) {
                        $options['order'][$options['map'][$key]] = $val;
                        unset($options['order'][$key]);
                    }
                }
            }
        }

        if (!isset($options['field'])) {
            $options['field'] = '*';
        }

        if (!isset($options['data'])) {
            $options['data'] = [];
        }

        if (!isset($options['strict'])) {
            $options['strict'] = $this->getConfig('fields_strict');
        }

        foreach (['master', 'lock', 'fetch_pdo', 'fetch_sql', 'distinct'] as $name) {
            if (!isset($options[$name])) {
                $options[$name] = false;
            }
        }

        foreach (['join', 'union', 'group', 'having', 'limit', 'order', 'force', 'comment'] as $name) {
            if (!isset($options[$name])) {
                $options[$name] = '';
            }
        }

        if (isset($options['page'])) {
            // 根据页数计算limit
            list($page, $listRows) = $options['page'];
            $page                  = $page > 0 ? $page : 1;
            $listRows              = $listRows > 0 ? $listRows : (is_numeric($options['limit']) ? $options['limit'] : 20);
            $offset                = $listRows * ($page - 1);
            $options['limit']      = $offset . ',' . $listRows;
        }

        // $this->options = [];
        $this->unsetField($this->options);
        return $options;
    }

    /**
     * 修改软删除的查询条件
     * @author Ultraman/2018-05-22
     * @param  array $options 查询条件
     * @return array          查询条件
     */
    protected function changeOptions($options)
    {
        $aliasArr = array_key_exists('alias', $options) ? $options['alias'] : null;
        //给主表添加delete_time=0条件
        $mainTable                                                                               = $options['table'];
        $mainAlias                                                                               = empty($aliasArr) ? null : $aliasArr[$mainTable];
        $options['where']['AND'][(empty($mainAlias) ? $mainTable : $mainAlias) . '.delete_time'] = array('EQ', 0);
        //给关联表添加delete_time=0条件
        if (!empty($options['join'])) {
            foreach ($options['join'] as $key => $value) {
                list($table, $type, $conditions) = $value;
                if (is_array($table)) {
                    list($null, $table) = each($table);
                }
                $conditions = $value[2];
                if (empty($conditions)) {
                    $conditions = $table . '.delete_time=0';
                } else {
                    if (is_array($conditions)) {
                        array_push($conditions, $table . '.delete_time=0');
                    } else {
                        $conditions .= ' AND ' . $table . '.delete_time=0';
                    }
                }
                $options['join'][$key][2] = $conditions;
            }
        }
        return $options;
    }

    /**
     * 重置field
     * @author Ultraman/2018-05-30
     * @param  array $options 查询参数
     * @return void
     */
    protected function unsetField(&$options)
    {
        if (isset($options['field'])) {
            unset($options['field']);
        }
    }
}
