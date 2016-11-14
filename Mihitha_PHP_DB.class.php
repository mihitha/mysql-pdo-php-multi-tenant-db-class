<?php

/*
 * Created By Mihitha Rajith Kankanamge
 * Copyrighted
 * Created 29 May 2015 (Version 1)
 * Update 21 Sep 2015 (Version 2)
 * Update 29 Jun 2016 (Version 3 - PDO Changeover)
 * All rights received.
 */

//This Database Class Uses MYSQL PDO Driver
class DATABASE_VER3 {

    public function __construct($tanent_id = 0) {
        //Load Database connection to an object
        try {
            $this->connection1 = new PDO('mysql:host=' . MYSQL_SERVER . ';dbname=' . MYSQL_DBASE . ';charset=utf8', MYSQL_USER, MYSQL_PASS);
            $this->connection2 = new PDO('mysql:host=' . MYSQL_SERVER . ';dbname=' . MYSQL_DBASE_PVT . ';charset=utf8', MYSQL_USER, MYSQL_PASS);
        } catch (Exception $e) {
            full_page_errors('Something is not right!', $e->getMessage());
            exit;
        }
        $this->check_for_multi_tanant = 'jail_me';
        
        //Load tanent ID
        if ((int) $tanent_id > 0) {
            $this->tanent_id = (int) $tanent_id;
        } else {
            $this->tanent_id = chk_login_user_id();
        }
    }

    /**
     * Database Connection for this Class
     * @return PDO Database Connection
     */
    private function Connect_to_PDO($database = '') {

        if ($database == '') {
            return $this->connection1;
        } else {
            $dbName0 = explode('.', $database);
            if (isset($dbName0[0]) && $dbName0[0] == MYSQL_DBASE_PVT) {
                return $this->connection2;
            } else {
                return $this->connection1;
            }
        }
    }

    /**
     * Inser Data to Database
     * @param string $table - Table Name
     * @param array $column_value_array - Array of data 'column_name'=>'value','column_name2'=>'value2'
     * @param String (Optional) $check_for_multi_tanant Multi Tenant Passcode
     * @return last insert ID
     * @return boolean false if fails
     * @author Mihitha R K <mihitha@gmail.com>
     */
    public function insert_me_in($table, $column_value_array, $check_for_multi_tanant = '') {

        $pdo = $this->Connect_to_PDO($table);

        //Multi Tanent
        if ($check_for_multi_tanant == $this->check_for_multi_tanant) {
            //Push tanant ID to the query
            $column_value_array['TANENT_ID'] = $this->tanent_id;
        }

        $keys = implode(',', array_keys($column_value_array));
        $values = ':' . implode(', :', array_keys($column_value_array));


        //Make Param Array in PDO way with :
        foreach ($column_value_array as $k => $v) {
            $column_value_array[':' . $k] = $v;
            unset($column_value_array[$k]);
        }

        //Create SQL statement
        $query = 'INSERT INTO ' . $table . ' (' . $keys . ') VALUES (' . $values . ')';

        //Set Errors
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);

        //PREPARE & EXECUTE QUERY
        $stmt = $pdo->prepare($query);
        if ($stmt->execute($column_value_array)) {
            return $pdo->lastInsertId();
        } else {
            $error_data = generateCallTrace();
            echo nl2br($error_data);
            return false;
        }
    }

    /**
     * Delete Data Set from Database
     * @param String $table Table Name
     * @param array $where_columns Where culumn list as key value array. Eg - array('column_name'=>'value','column_name2'=>'value2')
     * @param String $and_or Select type in between where columns. Either AND or OR. Cannot use mixed. 
     * @param String $check_for_multi_tanant (Optional) Enter Passcode to bypass multi tanent validation 
     * @return boolean True or False
     * @author Mihitha R K <mihitha@gmail.com>
     */
    public function delete_me_now($table, $where_columns, $and_or, $check_for_multi_tanant = '', $where_column_operator = '=') {
        $pdo = $this->Connect_to_PDO($table);
        //Multi Tanent
        if ($check_for_multi_tanant == $this->check_for_multi_tanant) {
            $where_columns['TANENT_ID'] = $this->tanent_id;
        }

        //WHERE COLUMN ARRAY COUNT
        $array_count = count($where_columns);

        //MAKE WHERE column string
        if ($array_count > 0) {
            $where_part = ' WHERE ';

            //Limit Last AND/OR of the loop
            $loop_count = 1;
            //Make "WHERE String of the QUERY"
            foreach ($where_columns as $key => $value) {
                if ($key == 'TANENT_ID') {
                    $where_part.=$key . '=' . ' :' . $key;
                } else {
                    //For all the other entris use normal $where_column_operator
                    $where_part.=$key . $where_column_operator . ' :' . $key;
                }
                if ($loop_count < $array_count) {
                    $where_part.=' ' . $and_or . ' ';
                }
                $loop_count++;
            }
        }

        //Make Param Array in PDO way with :
        foreach ($where_columns as $k => $v) {
            $where_columns[':' . $k] = $v;
            unset($where_columns[$k]);
        }

        //Make QUERY
        $query = 'DELETE FROM ' . $table . $where_part;
        //Set Errors
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);

        //PREPARE & EXECUTE QUERY
        $stmt = $pdo->prepare($query);
        if ($stmt->execute($where_columns)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Update Data in the Database
     * @param string $table - Table Name
     * @param string $which_column - Search Column for Update
     * @param string $search_value - Search Value
     * @param array $column_value_array - Array of data 'column_name'=>'value','column_name2'=>'value2'
     * @return boolean
     * @author Mihitha R K <mihitha@gmail.com>
     */
    public function update_me_now($table, $which_column, $search_value, $column_value_array, $check_for_multi_tanant = '', $where_column_operator = '=') {
        $pdo = $this->Connect_to_PDO($table);

        //Multi Tanent
        if ($check_for_multi_tanant != $this->check_for_multi_tanant) {
            $select_tanent = '';
        } else {
            $select_tanent = 'AND TANENT_ID = \'' . $this->tanent_id . '\'';
        }

        //WHERE COLUMN ARRAY COUNT
        $array_count = count($column_value_array);

        //MAKE SET column string
        if ($array_count > 0) {
            $where_part = ' SET ';

            //Limit Last AND/OR of the loop
            $loop_count = 1;
            //Make "WHERE String of the QUERY"
            foreach ($column_value_array as $key => $value) {
                $where_part.=$key . '=' . ' :' . $key;
                if ($loop_count < $array_count) {
                    $where_part.=', ';
                }
                $loop_count++;
            }
        }

        //Make Param Array in PDO way with :
        foreach ($column_value_array as $k => $v) {
            $column_value_array[':' . $k] = $v;
            unset($column_value_array[$k]);
        }
        //Make QUERY
        $query = 'UPDATE ' . $table . $where_part . ' WHERE ' . $which_column . $where_column_operator . '\'' . $search_value . '\' ' . $select_tanent;

        //Set Errors
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);

        //PREPARE & EXECUTE QUERY
        $stmt = $pdo->prepare($query);
        if ($stmt->execute($column_value_array)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Select Data Set from Database
     * @param String $memcahed_key Cache Key Name. (Optional. can leave '')
     * @param int $cache_length Cache Length in Seconds
     * @param String $table Table Name
     * @param String $prefix SQL Statement Prefix. Eg - DISTINCT
     * @param array $selected_columns Slected columns as a simple array. Eg - array(id,name)
     * @param array $where_columns Where culumn list as key value array. Eg - array('column_name'=>'value','column_name2'=>'value2')
     * @param String $and_or Select type in between where columns. Either AND or OR. Cannot use mixed. 
     * @param String $suffix SQL Statement Suffix. Eg - LIMIT 1
     * @param String $check_for_multi_tanant (Optional) Enter Passcode to bypass multi tanent validation
     * @param String $where_column_operator Comparison Operator
     * @return Array Multi Dimention Array
     * @author Mihitha R K <mihitha@gmail.com>
     */
    public function take_me_out($memcahed_key, $cache_length, $table, $prefix, $selected_columns, $where_columns, $and_or, $suffix, $check_for_multi_tanant = '', $where_column_operator = '=') {
        //Convert Select array to a String
        $select_string = implode(',', $selected_columns);

        //Parse empty $where_columns to an array
        if ($where_columns == '') {
            $where_columns = array('');
        }

        //Multi Tanent
        if ($check_for_multi_tanant != $this->check_for_multi_tanant) {
            $cache_key_suffix = '';
        } else {
            $where_columns['TANENT_ID'] = $this->tanent_id;
            $cache_key_suffix = $this->tanent_id;
        }

        //It is critical to mention Tanent ID in memcached key. otherwise key confilict may happen with other tanants
        //Data Caching
        global $memcached;
        //$memcached->flush(1);
        $cache_length1 = (int) $cache_length;
        //Cache or Not
        $need_cache = true;

        //Create unique key for Query string
        if ($need_cache && $cache_length1 > 0) {
            //Memcachae unique query key, if $cache_key_name empty
            if ($memcahed_key == '') {
                $memcahed_key = md5($table . $prefix . json_encode($selected_columns) . json_encode($where_columns) . $and_or . $suffix . $check_for_multi_tanant . $where_column_operator) . '_' . $cache_key_suffix;
            }
            if ($memcached->get($memcahed_key)) {
                return $memcached->get($memcahed_key);
                exit;
            }
        }

        //Database Connection
        $pdo = $this->Connect_to_PDO($table);

        //WHERE COLUMN ARRAY COUNT
        $array_count = count($where_columns);

        //MAKE WHERE column string
        if ($array_count > 0) {
            $where_part = ' WHERE ';

            //Limit Last AND/OR of the loop
            $loop_count = 1;
            //Make "WHERE String of the QUERY"
            foreach ($where_columns as $key => $value) {
                //Tanent ID enforcement
                //for TANENT_ID $where_column_operator shoud be =, even if it inserted as otherwise
                if ($key == 'TANENT_ID') {
                    $where_part.=$key . '=' . ' :' . $key;
                } else {
                    //For all the other entris use normal $where_column_operator
                    $where_part.=$key . $where_column_operator . ' :' . $key;
                }
                if ($loop_count < $array_count) {
                    $where_part.=' ' . $and_or . ' ';
                }
                $loop_count++;
            }
        }

        //Make Param Array in PDO way with :
        foreach ($where_columns as $k => $v) {
            $where_columns[':' . $k] = $v;
            unset($where_columns[$k]);
        }

        //Make QUERY
        $query = 'SELECT ' . $prefix . ' ' . $select_string . ' FROM ' . $table . $where_part . ' ' . $suffix;

        //Location Trace
//        echo '<br>';
//        echo generateCallTrace();
//        echo '<hr>';
        //Set Errors
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);

        //PREPARE & EXECUTE QUERY
        $stmt = $pdo->prepare($query);
        if ($stmt->execute($where_columns)) {
            $GLOBALS["func_count"] ++;
            //RETURN DATASET FROM FUNCTION as ASSOC ARRAY
            $dataset = $stmt->fetchAll(PDO::FETCH_ASSOC);

            //Caching
            if ($need_cache && $cache_length > 0) {
                $memcached->set($memcahed_key, $dataset, $cache_length);
            }

            return $dataset;
        } else {
            $error_data = generateCallTrace();
            echo nl2br($error_data);
        }
    }

    /**
     * Execute Custom Query
     * @param String $table MySQL Table Name
     * @param String $query MySQL Query
     * @param boolean $query_return_data [Optional] Set this to true if you need to query return data
     * @return boolean
     * @author Mihitha R K <mihitha@gmail.com>
     */
    public function custom_query($table, $query, $query_return_data = false) {

        $pdo = $this->Connect_to_PDO($table);

        //Set Errors
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);

        //PREPARE & EXECUTE QUERY
        $stmt = $pdo->prepare($query);
        if ($stmt->execute()) {
            //RETURN DATASET FROM FUNCTION as ASSOC ARRAY
            if ($query_return_data) {
                return $stmt->fetchAll(PDO::FETCH_ASSOC);
            } else {
                return true;
            }
        } else {
            $error_data = generateCallTrace();
            echo nl2br($error_data);
            return false;
        }
    }

}