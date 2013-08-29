<?php
/******************************************************************************\
 * @Version:    0.1
 * @Name:       TextOfDatabase
 * @Date:       2013-08-30 06:31:48 +08:00
 * @File:       TextOfDatabase.php
 * @Author:     Jak Wings <jakwings@gmail.com>
 * @License:    GPLv3
 * @Compatible: Apache/2.x with PHP/5.3.2+,5.4.x,5.5.x
 * @Thanks to:  pjjTextBase <http://pjj.pl/pjjtextbase/>
 *              txtSQL <http://txtsql.sourceforge.net>
\******************************************************************************/


class Todb
{
  const VERSION = '0.1';
  /**
  * @info   Database directory
  * @type   string
  */
  private $_db_path = NULL;
  /**
  * @info   Connected to a database?
  * @type   bool
  */
  private $_is_connected = FALSE;
  /**
  * @info   Database locked?
  * @type   bool
  */
  private $_is_locked = NULL;
  /**
  * @info   Show errors?
  * @type   bool
  */
  private $_debug = FALSE;
  private $_error_reporting_level = NULL;
  /**
  * @info   Cache of read tables
  * @type   array
  */
  private $_cache = array();
  /**
  * @info   working tables
  * @type   array
  */
  private $_tables = array();

  /**
  * @info   Constructor
  * @return {Todb}
  */
  public function __construct()
  {
    $this->_error_reporting_level = @error_reporting();
  }

  /**
  * @info   Open debug mode?
  * @param  {Boolean} $on: whether to show error message
  * @return void
  */
  public function Debug($on)
  {
    $on = !!$on;
    $this->_debug = $on;
    if ( $on ) {
      @error_reporting(E_ALL ^ E_WARNING ^ E_NOTICE);
    } else {
      if ( !is_null($this->_error_reporting_level) ) {
        @error_reporting($this->_error_reporting_level);
      }
    }
  }
  /**
  * @info   Connect to the database
  * @param  {String}  $path: (optional) database
  * @param  {Boolean} $toLock: (optional) not implemented yet
  * @return void
  */
  public function Connect($path = './db', $toLock = FALSE)
  {
    if ( $this->_is_connected ) {
      $this->_Error('OPERATION_ERROR', 'Not disconnected from previous database');
    }
    if ( FALSE === ($realpath = realpath(dirname($path . '/.'))) ) {
      $this->_Error('FILE_ERROR', 'Database not found');
    }
    $this->_db_path = $realpath;
    $this->_is_connected = TRUE;
  }
  /**
  * @info   Disconnect from the database
  * @param  void
  * @return void
  */
  public function Disconnect()
  {
    $this->_NeedConnected();
    $this->_cache = array();
    $this->_tables = array();
    $this->_is_connected = FALSE;
    unset($this->_db_path, $this->_is_locked);
  }
  /**
  * @info   Is connected to the database?
  * @param  void
  * @return {Boolean}
  */
  public function IsConnected()
  {
    return $this->_is_connected;
  }
  /**
  * @info   Is database locked?
  * @param  void
  * @return {Boolean}
  */
  public function IsLocked()
  {
    $this->_NeedConnected();
    return $this->_is_locked;
  }
  /**
  * @info   Lock database
  *         Return TRUE for success, or FALSE for failure
  * @param  void
  * @return {Boolean}
  */
  public function Lock()
  {
    $this->_NeedConnected();
  }
  /**
  * @info   Unlock database
  *         Return TRUE for success, or FALSE for failure
  * @param  void
  * @return {Boolean}
  */
  public function Unlock()
  {
    $this->_NeedConnected();
  }
  /**
  * @info   Return names of all table in the database if $tname isn't {String}
  *         or to see if table $tname exists
  * @param  {String}  $tname: (optional) table name to find
  * @return {Array}
  *         {Boolean}
  */
  public function ListTables($tname = NULL)
  {
    $this->_NeedConnected();
    $tables = array();
    if ( FALSE === ($dh = opendir($this->_db_path)) ) {
      $this->_Error('FILE_ERROR', 'Database not found');
    }
    while ( FALSE !== ($fname = readdir($dh)) ) {
      if ( is_file($this->_db_path . '/' . $fname) ) {
        $info = pathinfo($fname);
        if ( 'col' === $info['extension'] ) {
          if ( is_file($this->_db_path . '/' . $info['filename'] . '.row') ) {
            $tables[] = $info['filename'];
          }
        }
      }
    }
    closedir($dh);
    if ( !is_null($tname) ) {
      $this->_NeedValidName($tname);
      return in_array($tname, $tables, TRUE);
    }
    return $tables;
  }
  /**
  * @info   Create table
  *         Return TRUE for success, or FALSE for failure
  * @param  {String}  $tname: name of table
  * @param  {Array}   $tdata: headers and records
  * @return {Boolean}
  */
  public function CreateTable($tname, $tdata)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    $this->_NeedValidTable($tdata);
    $headers = $tdata['headers'];
    foreach ( $tdata['records'] as &$record ) {
      if ( FALSE === ($record = array_combine($headers, $record)) ) {
        $this->_Error('USER_ERROR', 'Invalid records');
      }
    }
    return $this->_WriteTable($tname, $tdata, FALSE, FALSE);
  }
  /**
  * @info   Delete table
  *         Return TRUE for success, or FALSE for failure
  * @param  {String}  $tname: name of table
  * @return {Boolean}
  */
  public function DropTable($tname)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    return $this->_UnlinkTable($tname);
  }
  /**
  * @info   Get headers of specified table
  * @param  {String}  $tname: name of specified table
  * @param  {Boolean} $fromFile: (optional) from the file or the working table
  * @return {Array}
  */
  public function GetHeaders($tname, $fromFile = FALSE)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    if ( !$fromFile ) {
      $this->_NeedFragmentLoaded($tname, TRUE);
      return $this->_tables[$tname . '.col'];
    }
    if ( FALSE === $this->_ReadHeaders($tname) ) {
      $this->_Error('FILE_ERROR', 'Table not found or broken');
    }
    return $this->_cache[$tname . '.col'];
  }
  /**
  * @info   Get the number of records of specified table
  *         Alias for simple use of Select()
  * @param  {String}  $tname: name of specified table
  * @param  {Closure} $where: (optional) a function that filter records
  * @param  {Boolean} $fromFile: (optional) from the file or the working table
  * @return {Integer}
  */
  public function Count($tname, $where = NULL, $fromFile = FALSE)
  {
    // positions of $fromFile and $where can be swapped
    // if there are only two arguments
    if ( func_num_args() === 2 and !is_callable($where) ) {
      $fromFile = $where;
      $where = NULL;
    }
    return $this->Select($tname, array(
      'action' => 'NUM',
      'where' => $where
    ), $fromFile);
  }
  /**
  * @info   Get the maximal value(s) of column(s) of records of specified table
  *         Alias for simple use of Select()
  *         Return NULL or array of NULLs if no value found
  * @param  {String}  $tname: name of specified table
  * @param  {String}  $column: specified header of a record
  *         {Array}   $column: specified headers of a record
  * @param  {Boolean} $fromFile: (optional) from the file or the working table
  * @return {Mixed}
  */
  public function Max($tname, $column, $fromFile = FALSE)
  {
    return $this->Select($tname, array(
      'action' => 'MAX',
      'column' => $column
    ), $fromFile);
  }
  /**
  * @info   Get the minimal value(s) of column(s) of records of specified table
  *         Alias for simple use of Select()
  *         Return NULL or array of NULLs if no value found
  * @param  {String}  $tname: name of specified table
  * @param  {String}  $column: specified header of a record
  *         {Array}   $column: specified headers of a record
  * @param  {Boolean} $fromFile: (optional) from the file or the working table
  * @return {Mixed}
  */
  public function Min($tname, $column, $fromFile = FALSE)
  {
    return $this->Select($tname, array(
      'action' => 'MIN',
      'column' => $column
    ), $fromFile);
  }
  /**
  * @info   Get the unique values of column(s) of records of specified table
  *         Alias for simple use of Select()
  *         Return array of or array of array
  * @param  {String}  $tname: name of specified table
  * @param  {String}  $column: specified header of a record
  *         {Array}   $column: specified headers of a record
  * @param  {Boolean} $fromFile: (optional) from the file or the working table
  * @return {Array}
  */
  public function Unique($tname, $column, $fromFile = FALSE)
  {
    return $this->Select($tname, array(
      'action' => 'UNI',
      'column' => $column
    ), $fromFile);
  }
  /**
  * @info   Get data of specified table that satisfies conditions
  * @param  {String}  $tname: name of specified table
  * @param  {Array}   $select: (optional) selection information
  *         -- by default, all select info are optional --
  *         'action'  => {arr} [ 'GET', 'NUM', 'MAX', 'MIN', 'SET', 'DEL'
  *                              'UNI', 'SET+', 'DEL+' ]
  *         'range'   => {arr} slice records before processing
  *         'where'   => {func} deal with every record (with referrence)
  *         'column'  => {str|arr} which column(s) of records to return
  *         'key'     => {str} use column value instead of number index
  *         'order'   => {arr} sort records by columns
  *         -- please mind your sever memory, do not deal with big data --
  * @param  {Boolean} $fromFile: (optional) from the file or the working table
  * @return {Mixed}
  */
  public function Select($tname, $select = NULL, $fromFile = FALSE)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    // positions of $fromFile and $where can be swapped
    // if there are only two arguments
    if ( func_num_args() === 2
      and !(is_null($select) or is_array($select)) )
    {
      $fromFile = $select;
      $select = NULL;
    }
    $this->_FormatSelectInfo($select, $fromFile);
    if ( !$fromFile ) {
      $this->_NeedFragmentLoaded($tname, FALSE);
      if ( is_null($select) ) {
        return $this->_tables[$tname . '.row'];
      }
      $records =& $this->_tables[$tname . '.row'];
    } else {
      $this->_NeedTable($tname, TRUE);
      if ( is_null($select) ) {
        return $this->_cache[$tname . '.row'];
      }
      $records =& $this->_cache[$tname . '.row'];
    }

    // basic info
    $where = $select['where'];
    $total = count($records);
    // set up range, like array_slice's (offset, length)
    $range = array();
    $range[0] = $select['range'][0] % $total;
    $range[0] = $range[0] < 0 ? $total + $range[0] : $range[0];
    $range[1] = $select['range'][1] % $total ?: $total;
    if ( $range[1] < 0 ) {
      $range[1] = $total + $range[1] + 1;
    } else {
      $range[1] = array_sum($range) > $total ? $total : array_sum($range);
    }

    switch ( $select['action'] ) {

      case 'GET':
        // get records within range
        $result = array();
        for ( list($i, $m) = $range; $i < $m; $i++ ) {
          $result[] = $records[$i];
        }
        // filter records
        if ( is_callable($where) ) {
          foreach ( $result as $index => $record ) {
            if ( !$where($record) ) {
              // indexes need re-mapping
              unset($result[$index]);
            }
          }
        }
        // sort records with specified order
        $this->_SortRecords($result, $select['order']);
        // only get data of specified column(s)
        $this->_SetColumn($result, $select['column'], $select['key']);
        // re-map indexes
        array_splice($result, 0, 0);
        return $result;

      case 'NUM':
        if ( is_callable($where) ) {
          $result = 0;
          for ( list($i, $m) = $range; $i < $m; $i++ ) {
            if ( $where($records[$i]) ) {
              $result++;
            }
          }
          return $result;
        }
        return $total;

      case 'MAX':
        $to_find_maximum = TRUE;
        // roll on
      case 'MIN':
        if ( is_array($select['column']) ) {
          $col_keys = $select['column'];
        } else {
          $to_single_value = TRUE;
          $col_keys = array($select['column']);
        }
        list($first_record) = array_slice($records, 0, 1);
        $col_keys = array_intersect($col_keys, array_keys($first_record));
        if ( count($col_keys) < 1 ) {
          return $to_single_value ? NULL : array();
        }
        $result = array_fill_keys($col_keys, NULL);
        foreach ( $col_keys as $key ) {
          $result[$key] = $first_record[$key];
        }
        if ( $to_find_maximum ) {
          foreach ( $col_keys as $key ) {
            for ( list($i, $m) = $range; $i < $m; $i++ ) {
              if ( is_null($where) || $where($records[$i])
                and $result[$key] < $records[$i][$key] )
              {
                $result[$key] =& $records[$i][$key];
              }
            }
          }
        } else {
          foreach ( $col_keys as $key ) {
            for ( list($i, $m) = $range; $i < $m; $i++ ) {
              if ( is_null($where) || $where($records[$i])
                and $result[$key] > $records[$i][$key] )
              {
                $result[$key] =& $records[$i][$key];
              }
            }
          }
        }
        return $to_single_value ? array_values($result)[0] : $result;

      case 'SET+':
        $to_return_records = TRUE;
        // roll on
      case 'SET':
        $records_cnt = 0;
        $selected_records = array();
        for ( list($i, $m) = $range; $i < $m; $i++ ) {
          if ( $where($records[$i]) ) {
            if ( $to_return_records ) {
              $selected_records[] = $records[$i];
            } else {
              $records_cnt++;
            }
          }
        }
        return $to_return_records ? $selected_records : $records_cnt;

      case 'DEL+':
        $to_return_records = TRUE;
        // roll on
      case 'DEL':
        $records_cnt = 0;
        $deleted_records = array();
        if ( is_callable($where) ) {
          for ( list($i, $m) = $range; $i < $m; $i++ ) {
            if ( $where($records[$i]) ) {
              if ( $to_return_records ) {
                $deleted_records[] = $records[$i];
              } else {
                $records_cnt++;
              }
              // indexes need re-mapping
              unset($records[$i]);
            }
          }
          // re-map indexes
          array_splice($records, 0, 0);
          return $to_return_records ? $deleted_records : $records_cnt;
        }
        $deleted_records = array_splice($records, 0);
        $records_cnt = $total;
        return $to_return_records ? $deleted_records : $records_cnt;

      case 'UNI':
        if ( is_array($select['column']) ) {
          $col_keys = $select['column'];
        } else {
          $to_single_value = TRUE;
          $col_keys = array($select['column']);
        }
        $result = array_fill_keys($col_keys, array());
        list($first_record) = array_slice($records, 0, 1);
        // invalid columns are left with empty array
        $col_keys = array_intersect($col_keys, array_keys($first_record));
        if ( count($col_keys) < 1 ) {
          return $to_single_value ? NULL : $result;
        }
        foreach ( $col_keys as $key ) {
          for ( list($i, $m) = $range; $i < $m; $i++ ) {
            if ( is_null($where) or $where($records[$i]) ) {
              $result[$key][] =& $records[$i][$key];
            }
          }
        }
        foreach ( $col_keys as $key ) {
          $result[$key] = array_unique($result[$key], SORT_REGULAR);
        }
        return $to_single_value ? array_values($result)[0] : $result;

      default:
        $this->_Error('SYNTAX_ERROR', 'Unknown error');
    }
  }
  /**
  * @info   Insert one record to a working table
  * @param  {String}  $tname: name of specified table
  * @param  {Array}   $record: one record to insert
  * @return void
  */
  public function Insert($tname, $record)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    $this->_NeedTable($tname, FALSE);
    //$this->_SortRecordValues($tname, $record, FALSE);
    $this->_NeedValidTable(array(
      'headers' => $this->_tables[$tname . '.col'],
      'records' => array($record)
    ));
    $this->_tables[$tname . '.row'][] = $record;
  }
  /**
  * @info   Merge records to a working table
  * @param  {String}  $tname: name of specified table
  * @param  {Array}   $records: records to merge with
  * @return void
  */
  public function Merge($tname, $records)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    $this->_NeedTable($tname, FALSE);
    //foreach ( $records as &$record ) {
    //  $this->_SortRecordValues($tname, $record, FALSE);
    //}
    $this->_NeedValidTable(array(
      'headers' => $this->_tables[$tname . '.col'],
      'records' => $records
    ));
    call_user_func_array('array_push', array(
      &$this->_tables[$tname . '.row'],
      $records
    ));
  }
  /**
  * @info   Overwrite records of a working table
  * @param  {String}  $tname: name of specified table
  * @param  {Array}   $records
  * @return void
  */
  public function SetRecords($tname, $records)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    $this->_NeedFragmentLoaded($tname, TRUE);
    $this->_NeedValidTable(array(
      'headers' => $this->_tables[$tname . '.col'],
      'records' => $records
    ));
    $this->_tables[$tname . 'row'] = $records;
  }
  /**
  * @info   Append record(s) directly to a table file
  *         Return TRUE for success, or FALSE for failure
  * @param  {String}  $tname: name of specified table
  * @param  {Array}   $records: record(s) to append
  * @param  {Boolean} $toRecords: not just one record?
  * @return {Boolean}
  */
  public function Append($tname, $records, $toRecords = FALSE)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    if ( FALSE === $this->_ReadHeaders($tname) ) {
      $this->_Error('FILE_ERROR', 'Table not found or broken');
    }
    //$this->_SortRecordValues($tname, $record, TRUE);
    if ( $toRecords ) {
      $records = array($records);
    }
    $headers = $this->_cache[$tname . '.col'];
    $this->_NeedValidTable(array(
      'headers' => $headers,
      'records' => $records
    ));
    return $this->_WriteTable($tname, array(
      'headers' => $headers,
      'records' => $records
    ), TRUE, FALSE);
  }
  /**
  * @info   Write a working table to the database.
  *         Return TRUE for success, or FALSE for failure
  * @param  {String}  $tname: name of specified table
  * @return {Boolean}
  */
  public function Update($tname)
  {
    $this->_NeedConnected();
    $this->_NeedValidName($tname);
    if ( !is_array($this->_tables[$tname . '.col'])
      or !is_array($this->_tables[$tname . '.row']) )
    {
      $this->_Error('OPERATION_ERROR', 'Table not loaded');
    }
    return $this->_WriteTable($tname, array(
      'headers' => $this->_tables[$tname . '.col'],
      'records' => $this->_tables[$tname . '.row']
    ), FALSE, TRUE);
  }
  /**
  * @info   Empty the cache of records
  *         Empty all cache of records if $tname is NULL.
  * @param  {String}  $tname: (optional) name of specified table
  * @return void
  */
  public function EmptyCache($tname = NULL)
  {
    $this->_NeedConnected();
    if ( !is_null($tname) ) {
      $this->_NeedValidName($tname);
      unset($this->_cache[$tname . '.row']);
      return;
    }
    $this->_cache = array();
  }

  private function _Error($errType, $errMsg)
  {
    if ( !$this->_debug ) {
      exit();
    }
    $errMsg = @htmlentities($errMsg, ENT_COMPAT, 'UTF-8');
    echo <<<"EOT"
<pre style="white-space:pre-wrap;color:#B22222;">
<b>{$errType}:</b><br>
  <b>{$errMsg}</b>
</pre>
EOT;
    echo '<pre style="white-space:pre-wrap">Backtrace:<br>';
    @debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
    echo '</pre>';
    exit();
  }
  private function _NeedConnected()
  {
    if ( !$this->_is_connected ) {
      $this->_Error('OPERATION_ERROR', 'Not connected to any database');
    }
  }
  private function _NeedValidName($tname)
  {
    if ( !is_string($tname) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid table name');
    }
  }
  private function _NeedTable($tname, $fromFile)
  {
    if ( FALSE === $this->_ReadTable($tname) ) {
      $this->_Error('FILE_ERROR', 'Table not found or broken');
    }
    if ( $fromFile ) {
      return;
    }
    if ( !is_array($this->_tables[$tname . '.col'])
      or !is_array($this->_tables[$tname . '.row']) )
    {
      $this->_tables[$tname . '.col'] = $this->_cache[$tname . '.col'];
      $this->_tables[$tname . '.row'] = $this->_cache[$tname . '.row'];
    }
  }
  private function _NeedFragmentLoaded($tname, $headers_or_records)
  {
    if ( $headers_or_records and !is_array($this->_tables[$tname . '.col']) ) {
      if ( FALSE === $this->_ReadHeaders($tname) ) {
        $this->_Error('FILE_ERROR', 'Table not found or broken');
      }
      $this->_tables[$tname . '.col'] = $this->_cache[$tname . '.col'];
    }
    if ( !$headers_or_records and !is_array($this->_tables[$tname . '.row']) ) {
      if ( FALSE === $this->_ReadTable($tname) ) {
        $this->_Error('FILE_ERROR', 'Table not found or broken');
      }
      $this->_tables[$tname . '.row'] = $this->_cache[$tname . '.row'];
    }
  }
  private function _FormatSelectInfo(&$select, $fromFile)
  {
    if ( !(is_null($select) or is_array($select)) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid select');
    }
    $valid_actions = array(
      'GET', 'NUM', 'MAX', 'MIN', 'SET', 'DEL', 'SET+', 'DEL+', 'UNI'
    );
    $select['action'] = strtoupper($select['action']) ?: 'GET';
    if ( !is_string($select['action'])
      or !in_array($select['action'], $valid_actions, TRUE) )
    {
      $this->_Error('SYNTAX_ERROR', 'Invalid select info "action"');
    }
    $select['range'] = $select['range'] ?: array();
    if ( !is_array($select['range'])
      or !(is_null($select['range'][0]) or is_integer($select['range'][0]))
      or !(is_null($select['range'][1]) or is_integer($select['range'][0])) )
    {
      $this->_Error('SYNTAX_ERROR', 'Invalid select info "offset"');
    }
    $select['where'] = $select['where'] ?: NULL;
    if ( !(is_null($select['where']) or is_callable($select['where'])) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid select info "where"');
    }
    $select['column'] = $select['column'] ?: array();
    if ( !(is_string($select['column']) or is_array($select['column'])) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid select info "column"');
    }
    $select['key'] = $select['key'] ?: '';
    if ( !is_string($select['key']) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid select info "key"');
    }
    $select['order'] = $select['order'] ?: NULL;
    if ( !(is_null($select['order']) or is_array($select['order'])) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid select info "order"');
    } else {
      $valid_sort_flags = array(SORT_ASC, SORT_DESC);
      foreach ( $select['order'] as $key => $flag ) {
        if ( !isset($key[0]) or !in_array($flag, $valid_sort_flags, TRUE) ) {
          $this->_Error('SYNTAX_ERROR', 'Invalid select info "order"');
        }
      }
    }
    if ( in_array($select['action'], array('SET', 'DEL', 'SET+', 'DEL+'), TRUE) )
    {
      if ( $fromFile ) {
        $this->_Error('SYNTAX_ERROR', 'Invalid select action');
      }
      if ( ($select['action'] === 'SET' or $select['action'] === 'SET+')
        and !is_callable($select['where']) )
      {
        $this->_Error('SYNTAX_ERROR', 'Invalid select action');
      }
    }
  }
  private function _SetColumn(&$records, $columnKeys, $indexKey)
  {
    if ( count($records) < 1 ) {
      return $records;
    }
    if ( is_string($columnKeys) ) {
      $columnKeys = isset($columnKeys[0]) ? array($columnKeys) : array();
    }
    list($first_record) = array_slice($records, 0, 1);
    $array_keys = array_keys($first_record);
    $column_keys = array_intersect($columnKeys, $array_keys);
    $has_index_key = !empty($indexKey);
    if ( $has_index_key ) {
      if ( !in_array($indexKey, $array_keys, TRUE) ) {
        $this->_Error('SYNTAX_ERROR', 'Invalid select info "key"');
      }
      $key_column_values = array();
      $column_keys = array_diff($column_keys ?: $array_keys, array($indexKey));
    }
    if ( !empty($column_keys) and count($column_keys) < count($first_record) ) {
      $other_keys = array_flip(array_diff($array_keys, $column_keys));
      $column_keys = array_flip($column_keys);
      $to_first_mode = count($other_keys) > (count($array_keys) / 2);
      $records_length = count($records);
      foreach ( $records as $index => &$record ) {
        if ( $has_index_key ) {
          $key_column_values[$index] =& $record[$indexKey];
        }
        if ( $to_first_mode ) {
          $records[$index] = array_intersect_key($record, $column_keys);
        } else {
          $records[$index] = array_diff_key($record, $other_keys);
        }
      }
      if ( $has_index_key ) {
        $new_records = array();
        foreach ( $records as $index => &$record ) {
          $new_records[$key_column_values[$index]] =& $record;
        }
        $records = $new_records;
      }
    }
  }
  private function _SortRecords(&$records, $sortFlags)
  {
    if ( count($records) < 1 or is_null($sortFlags) ) {
      return;
    }
    list($first_record) = array_slice($records, 0, 1);
    $array_keys = array_keys($first_record);
    $sort_flags = array_intersect_key($sortFlags, array_flip($array_keys));
    $sort_keys = array_keys($sort_flags);
    if ( count($sort_flags) < 1 ) {
      return;
    }
    $columns = array();
    $sort_args = array();
    foreach ( $sort_keys as $key ) {
      foreach ( $records as $record ) {
        $columns[$key][] =& $record[$key];
      }
      $sort_args[] =& $columns[$key];
      $sort_args[] =& $sort_flags[$key];
    }
    $sort_args[] =& $records;
    call_user_func_array('array_multisort', $sort_args);
  }
  private function _GetSecureFileName($name)
  {
    if ( FALSE !== strpos('/' . $name, '/..') ) {
      $this->_Error('OPERATION_ERROR', 'Insecure file name');
    }
    return $this->_db_path . '/' . $name;
  }
  //private function _SortRecordValues($tname, &$record, $fromFile)
  //{
  //  if ( $fromFile ) {
  //    $headers = $this->_cache[$tname . '.col'],
  //  } else {
  //    $headers = $this->_tables[$tname . '.col'],
  //  }
  //  $new_record = array();
  //  foreach ( $headers as $header ) {
  //    if ( isset($record[$header]) or array_key_exists($header, $record) ) {
  //      $new_record[$header] = $record[$header];
  //    }
  //  }
  //  $record = $new_record;
  //}
  private function _FilterInput(&$val, $key)
  {
    if ( is_string($val) ) {
      $val = str_replace("\x00", '', $val);
    }
  }
  private function _IsValidHeaders($headers)
  {
    if ( !is_array($headers) or count($headers) < 1 ) {
      return FALSE;
    }
    foreach ( $headers as $header ) {
      // header must be non-empty string
      if ( !is_string($header) or preg_match('/^\\d*$/', $header) ) {
        return FALSE;
      }
    }
    return TRUE;
  }
  private function _IsValidRecords($records, $length)
  {
    if ( !is_array($records) ) {
      return FALSE;
    }
    foreach ( $records as $record ) {
      if ( !is_array($record) or $length !== count($record) ) {
        return FALSE;
      }
    }
    return TRUE;
  }
  private function _NeedValidTable($tdata)
  {
    if ( !is_array($tdata) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid table');
    }
    // 1-D Array: names of headers
    $headers = $tdata['headers'];
    if ( !$this->_IsValidHeaders($headers) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid headers');
    }
    // 2-D Array: header-value dicts of records
    $records = $tdata['records'] ?: array();
    if ( !$this->_IsValidRecords($records, count($headers)) ) {
      $this->_Error('SYNTAX_ERROR', 'Invalid records');
    }
  }
  private function _ReadHeaders($tname, $waitIfLocked = TRUE)
  {
    // find cache
    if ( is_array($this->_cache[$tname . '.col']) ) {
      // 1-D Array: names of headers
      return TRUE;
    }
    $filename = $this->_GetSecureFileName($tname);
    if ( !is_readable($filename . '.col') ) {
      $this->_Error('FILE_ERROR', 'Table not found or broken');
    }
    $fh_col = @fopen($filename . '.col', 'rb');
    if ( $waitIfLocked ) {
      $is_locked = @flock($fh_col, LOCK_SH, $would_block);
    } else {
      $is_locked = @flock($fh_col, LOCK_SH | LOCK_NB, $would_block);
    }
    //if ( $would_block && !$waitIfLocked or !$is_locked ) {
    if ( !$is_locked ) {
      @fclose($fh_col);
      return FALSE;
    } else {
      $cts_col = @file_get_contents($filename . '.col');
      @flock($fh_col, LOCK_UN);
      @fclose($fh_col);
      $headers = unserialize($cts_col);
      if ( FALSE === $headers or !is_array($headers) or count($headers) < 1 ) {
        $this->_Error('FILE_ERROR', 'Broken data');
      }
      $this->_cache[$tname . '.col'] = $headers;
    }
    return TRUE;
  }
  private function _ReadRecords($tname, $waitIfLocked = TRUE)
  {
    // find cache
    if ( is_array($this->_cache[$tname . '.row']) ) {
      // 2-D Array: header-value dicts of records
      return TRUE;
    }
    $filename = $this->_GetSecureFileName($tname);
    if ( !is_readable($filename . '.row') ) {
      $this->_Error('FILE_ERROR', 'Table not found or broken');
    }
    $fh_row = @fopen($filename . '.row', 'rb');
    if ( $waitIfLocked ) {
      $is_locked = @flock($fh_row, LOCK_SH, $would_block);
    } else {
      $is_locked = @flock($fh_row, LOCK_SH | LOCK_NB, $would_block);
    }
    //if ( $would_block && !$waitIfLocked or !$is_locked ) {
    if ( !$is_locked ) {
      @fclose($fh_row);
      return FALSE;
    }
    $lines = @file($filename . '.row', FILE_IGNORE_NEW_LINES); // without EOL
    // remove blank lines cause by previous appending to empty file
    $lines_length = count($lines);
    while ( $lines_length > 0 and empty($lines[0]) ) {
      array_shift($lines);
      $lines_length--;
    }
    @flock($fh_row, LOCK_UN);
    @fclose($fh_row);
    $cts_rlines = array();
    $cts_rindex = -1;
    foreach ( $lines as $line ) {
      if ( "\x00" === $line[0] ) {
        $cts_rindex++;
        $cts_rlines[] = substr($line, 1);
      } else {
        $cts_rlines[$cts_rindex] .= PHP_EOL . $line;
      }
    }
    $records = array();
    $headers = $this->_cache[$tname . '.col'];
    $headers_length = count($headers);
    foreach ( $cts_rlines as $cts_rline ) {
      $record = unserialize($cts_rline);
      if ( FALSE === $record
        or !is_array($record)
        or $headers_length !== count($record) )
      {
        $this->_Error('FILE_ERROR', 'Broken data');
      }
      $records[] = array_combine($headers, $record);
    }
    $this->_cache[$tname . '.row'] = $records;
    return TRUE;
  }
  private function _ReadTable($tname, $waitIfLocked = TRUE)
  {
    if ( !$this->_ReadHeaders($tname, $waitIfLocked)
      or !$this->_ReadRecords($tname, $waitIfLocked) ) {
      return FALSE;
    }
    return TRUE;
  }
  private function _WriteTable($tname, $tdata, $toAppend, $toOverwrite)
  {
    // can't be both true
    if ( $toAppend and $toOverwrite ) {
      return FALSE;
    }
    $filename = $this->_GetSecureFileName($tname);
    if ( !($toOverwrite or $toAppend)
      and (is_file($filename . '.col') || is_file($filename . '.row')) )
    {
      return FALSE;
    }
    if ( $toOverwrite || $toAppend
      and (!is_writable($filename . '.col')
        or !is_writable($filename . '.row')) )
    {
      return FALSE;
    }
    @ignore_user_abort(TRUE);
    // write headers
    if ( !$toAppend ) {
      array_walk($tdata['headers'], 'self::_FilterInput');
      if ( FALSE === @file_put_contents($filename . '.col', serialize($tdata['headers']), LOCK_EX) ) {
        return FALSE;
      }
    }
    // write records
    array_walk_recursive($tdata['records'], 'self::_FilterInput');
    $fh_write_mode = $toAppend ? 'ab' : 'wb';
    $fh_row = @fopen($filename . '.row', $fh_write_mode, LOCK_EX);
    $records_ix_max = count($tdata['records']) - 1;
    for ( $i = 0; $i <= $records_ix_max; $i++ ) {
      $record = $tdata['records'][$i];
      $line = $toAppend ? PHP_EOL : '';
      $line .= "\x00" . serialize(array_values($record));
      if ( $i < $records_ix_max ) {
        $line .= PHP_EOL;
      }
      if ( FALSE === @fwrite($fh_row, $line) ) {
        @flock($fh_row, LOCK_UN);
        @fclose($fh_row);
        @ignore_user_abort(FALSE);
        return FALSE;
      }
    }
    @flock($fh_row, LOCK_UN);
    @fclose($fh_row);
    @ignore_user_abort(FALSE);
    if ( $toOverwrite ) {
      $this->_cache[$tname . '.col'] = $tdata['headers'];
      $this->_cache[$tname . '.row'] = $tdata['records'];
    }
    return TRUE;
  }
  private function _UnlinkTable($tname)
  {
    $filename = $this->_GetSecureFileName($tname);
    if ( !(is_file($filename . '.col') or is_file($filename . '.row')) ) {
      return FALSE;
    }
    if ( !@unlink($filename . '.col') and !@unlink($filename . '.row') ) {
      return FALSE;
    }
    $this->EmptyCache($tname);
    return TRUE;
  }
}
?>
