<?php
class fias 
{
  protected $dir = null;
  
  protected $db = null;
  protected $table_prefix = null;
  protected $max_insert_block = null; 
  
  public $debug = false;
  
  public $indexTable = ['addrob' => ['PRIMARY KEY' => 'aoid',
                                        'INDEX' => ['aoid', 'aoguid', 'formalname', 'code', 'aolevel', 'parentguid']],
                        'house' => ['PRIMARY KEY' => 'houseid',
                                    'INDEX' => ['aoguid', 'houseguid']],
                        'stead' => ['PRIMARY KEY' => 'steadguid'],
                        'room' => ['PRIMARY KEY' => 'roomguid'],
                        'houseint' => ['PRIMARY KEY' => 'houseintid'],
                        'nordoc' => ['PRIMARY KEY' => 'docimgid'],
                        'socrbase' => ['PRIMARY KEY' => 'kod_t_st'],
                        'room' => ['PRIMARY KEY' => 'roomguid'],
                        'curentst' => ['PRIMARY KEY' => 'curentstid'],
                      ];
  public $engine = 'MyISAM';
  public $collate = 'utf8_general_ci';
  public $convertFrom = 'CP866';
  
  protected $fileName = null;
  protected $fp = null;
  protected $filePos = 0;
  protected $recordPos = -1;
  protected $record;

  public $version;
  public $recordCount;
  public $recordByteLength;
  public $inTransaction;
  public $encrypted;
  public $columns;
  public $headerLength;
  public $backlist;
  public $foxpro;
  public $memoFile;
    
  const MEMO = 'M';
  const CHAR = 'C';
  const DOUBLE = 'B';
  const NUMERIC = 'N';
  const FLOATING = 'F';
  const DATE = 'D';
  const LOGICAL = 'L';
  const DATETIME = 'T';
  const INDEX = 'I';
  const IGNORE_0 = '0';
  
  public function __construct($dir, $dbconfig, $table_prefix = '', $max_insert_block = 1000){
      $this->dir = $dir;
      $this->db = new mysqli($dbconfig['host'], $dbconfig['username'], $dbconfig['password'], $dbconfig['dbname']);
      $this->table_prefix = $table_prefix;
      $this->max_insert_block = $max_insert_block;
  }
  public function import() {
    if($this->debug){
      echo "Strat\n";
    }
    $files = scandir($this->dir);
    foreach ($files as $file) {
      if($file != '.' && $file != '..' && strtolower(pathinfo($file, PATHINFO_EXTENSION)) == 'dbf'){
        $this->fileName = $this->dir . $file;
        if($this->debug){
          echo "File import: ".$this->fileName."\n";
        }
        $this->tableName = $this->table_prefix . strtolower(pathinfo($file, PATHINFO_FILENAME));
        $this->open();

        $types = []; $col = []; $values = [];
        while ($record = $this->nextRecord()) {
          if(count($col) == 0){
            $col = $this->getColumnsName();
            if(!$this->tableExists($this->tableName)){
              $this->createTable();
            }
          }
          $val = [];
          foreach ($this->columns as $column) {
            switch ($column['type']) {
              case self::CHAR: $val[] = "'".$this->getString($column['name'])."'";break;
              case self::NUMERIC:$val[] = (int) $this->getNum($column['name']);break;
              case self::DATETIME: $val[] = $this->getString($column['name']);break;
              case self::DATE: $val[] = $this->getString($column['name']); break;      
              default:$val[] = "'".$this->getObject($column)."'";break;
            }
          }
          if(count($val)) $values[] = '('.implode(',',$val).')';
          if(count($values) >= $this->max_insert_block){
            $this->insert($this->tableName, $col, $values);
            $values = [];
            if($this->debug && $this->recordCount > 0){
              echo ceil($this->recordPos * 100 / $this->recordCount)."%\n";
            }
          }
        }
        $this->close();
        if(!$this->debug){
          unlink($this->dir . $file);
        }
        if(count($values)){
          $this->insert($this->tableName, $col, $values);
        }
      }
    }
    if($this->debug){
      echo "Finish\n";
      echo "Bay!!!\n";
    }
  }
  protected function insert($table,$col,$values){
    $this->db->query('INSERT IGNORE INTO `'.$table.'` (`'.implode('`,`',$col).'`) VALUES '.implode(',',$values));
  }
  protected function createTable(){
      foreach ($this->columns as $column) {
          $type = false;
          switch ($column['type']) {
          case self::CHAR: 
              if($column['length'] <= 255){
                $type = 'CHAR('. $column['length'].')'; break;
              }else{
                $type = 'VARCHAR('. $column['length'].')'; break;
              }
          case self::DOUBLE: $type = 'DOUBLE('. $column['length'].')'; break;
          case self::NUMERIC: 
              if($column['length'] <= 2){
                $type = 'TINYINT('. $column['length'].')';
              }elseif($column['length'] <= 4){
                $type = 'SMALLINT('. $column['length'].')';
              }elseif($column['length'] <= 6){
                $type = 'MEDIUMINT('. $column['length'].')';
              }elseif($column['length'] <= 9){
                $type = 'INT('. $column['length'].')'; 
              }else{
                $type = 'BIGINT('. $column['length'].')'; 
              }
              break;
          case self::FLOATING: $type = 'FLOAT('. $column['length'].')'; break;
          case self::DATETIME: $type = 'DATETIME'; break;
          case self::DATE: $type = 'DATE'; break;          
        };
        if($type){
          if(isset($this->indexTable[$baseName]['PRIMARY KEY']) && $this->indexTable[$baseName]['PRIMARY KEY'] == $column['name']){
            $types[] = '`'.$column['name'].'` '.$type.' NULL DEFAULT NULL';
          }else{
            $types[] = '`'.$column['name'].'` '.$type;
          }
        }
    }
    $query = 'CREATE TABLE `'.$this->tableName.'` ( '.implode(',',$types);
    $baseName = preg_replace('/\d/','',$this->tableName);
    if(isset($this->indexTable[$baseName]['PRIMARY KEY'])){
      $query .= ', PRIMARY KEY (`'.$this->indexTable[$baseName]['PRIMARY KEY'].'`)';
    }
    if(isset($this->indexTable[$baseName]['INDEX'])){
      $query .= ', INDEX `'.$this->indexTable[$baseName]['INDEX'][0].'` (`'.implode('`, `',$this->indexTable[$baseName]['INDEX']).'`)';
    }
    $query .= ') COLLATE=\''.$this->collate.'\' ENGINE='.$this->engine;
    $this->db->query($query);
  }
  protected function getColumnsName(){
    $result = [];
    foreach ($this->columns as $column) {
      $result[$column['name']] = $column['name'];
    }
    return $result;
  }

  /*
  * DBF Functions
  */
  protected function open(){
      if (!file_exists($this->fileName)) {
          throw new \Exception(sprintf('File %s cannot be found', $this->fileName));
      }
      $this->fp = fopen($this->fileName, 'rb');
      $this->version = $this->readChar();
      $this->foxpro = in_array($this->version, array(48, 49, 131, 203, 245, 251));
      $this->read3ByteDate();
      $this->recordCount = $this->readInt();
      $this->headerLength = $this->readShort();
      $this->recordByteLength = $this->readShort();
      $this->readBytes(2);
      $this->inTransaction = $this->readByte()!=0;
      $this->encrypted = $this->readByte()!=0;
      $this->readBytes(4);
      $this->readBytes(8);
      $this->readByte();
      $this->readByte();
      $this->readBytes(2);
      $fieldCount = floor(($this->headerLength - ($this->foxpro ? 296 : 33)) / 32);
      if ($this->headerLength > filesize($this->fileName)) {
          throw new \Exception(sprintf('File %s is not DBF', $this->fileName));
      }
      if ($this->headerLength + ($this->recordCount * $this->recordByteLength) - 500 > filesize($this->fileName)) {
          throw new \Exception(sprintf('File %s is not DBF', $this->fileName));
      }
      $this->columns = array();
      $bytepos = 1;
      $j = 0;
      for ($i = 0; $i < $fieldCount ; $i++) {
          $column = $this->column(
              strtolower($this->readString(11)),
              $this->readByte(),
              $this->readInt(),
              $this->readChar(),
              $this->readChar(),
              $this->readBytes(2),
              $this->readChar(),
              $this->readBytes(2),
              $this->readByte()!=0,
              $this->readBytes(7),
              $this->readByte()!=0,
              $j,
              $bytepos
          );
          $bytepos += $column['length'];
          if (!$this->avaliableColumns || ($this->avaliableColumns && in_array($column->name, $this->avaliableColumns))) {
              $this->addColumn($column);
              $j++;
          }
      }
      if ($this->foxpro) {
          $this->backlist = $this->readBytes(263);
      }
      $this->setFilePos($this->headerLength);
      $this->recordPos = -1;
      $this->record = false;
      return $this->fp != false;
  }
  public function close(){
      fclose($this->fp);
  }
  /*
  * Record Functions
  */
  public function record($recordIndex, $rawData = false){
      $this->recordIndex = $recordIndex;
      $this->choppedData = array();
      if($rawData && strlen($rawData) > 0){
          foreach ($this->columns as $column){
              $this->choppedData[$column['name']] = substr($rawData, $column['bytePos'], $this->getDataLength($column));
          }
      }else{
          foreach($this->columns as $column){
              $this->choppedData[$column['name']] = str_pad('', $this->getDataLength($column), chr(0));
          }
      }
      return $this->choppedData;
  }
  public function nextRecord(){
      if ($this->record){
          $this->destroy();
          $this->record = null;
      }
      if (($this->recordPos + 1) >= $this->recordCount){
          return false;
      }
      $this->recordPos++;
      $this->record = $this->record($this->recordPos, $this->readBytes($this->recordByteLength));
      return $this->record;
  }
  public function destroy(){
      $this->choppedData = null;
  }
  /*
  * Column Functions
  */
  public function column($name, $type, $memAddress, $length, $decimalCount, $reserved1, $workAreaID, $reserved2, $setFields, $reserved3, $indexed, $colIndex, $bytePos){
      $column = [];
      $column['rawname'] = $name;
      $column['name'] = (strpos($name, 0x00) !== false ) ? substr($name, 0, strpos($name, 0x00)) : $name;
      $column['type'] = $type;
      $column['memAddress'] = $memAddress;
      $column['length'] = $length;
      $column['decimalCount'] = $decimalCount;
      $column['workAreaID'] = $workAreaID;
      $column['setFields'] = $setFields;
      $column['indexed'] = $indexed;
      $column['bytePos'] = $bytePos;
      $column['colIndex'] = $colIndex;
      return $column;
  }
  public function getColumn($name){
      foreach ($this->columns as $column){
          if ($column['name'] === $name){
              return $column;
          }
      }
      throw new \Exception(sprintf('Column %s not found', $name));
  }
  public function addColumn($column){
      $name = $nameBase = $column['name'];
      $index = 0;
      while (isset($this->columns[$name])){
          $name = $nameBase . ++$index;
      }
      $column['name'] = $name;
      $this->columns[$name] = $column;
  }
  /*
  * Read Functions
  */
  public function getNum($columnName){
      $s = $this->forceGetString($columnName);
      if (!$s){
          return false;
      }
      $s = str_replace(',', '.', $s);
      $column = $this->getColumn($columnName);
      if ($column->type == self::NUMERIC && ($column->getDecimalCount() > 0 || $column->length > 9)){
          return doubleval($s);
      }else{
          return intval($s);
      }
  }
  public function getString($columnName){
      $column = $this->getColumn($columnName);
      if ($column['type'] == self::CHAR) {
          return $this->forceGetString($columnName);
      } else {
          $result = $this->getObject($column);
          if ($result && $column['type'] == self::DATETIME) {
              return date("Y-m-d H:i:s", $result);
          }
          if($column['type'] == self::DATE){
            return date("Y-m-d", $result);
          }
          if ($column['type'] == self::LOGICAL) {
              return $result? '1' : '0';
          }
          return $result;
      }
  }
  public function getDate($columnName){
      $s = $this->forceGetString($columnName);
      if (!$s) {
          return false;
      }
      return strtotime($s);
  }
  
  public function getObject($column){
      switch ($column['type']) {
          case self::CHAR:return $this->getString($column['name']);
          case self::DOUBLE:return $this->getDouble($column['name']);
          case self::DATE:return $this->getDate($column['name']);
          case self::DATETIME:return $this->getDateTime($column['name']);
          case self::FLOATING:return $this->getFloat($column['name']);
          case self::LOGICAL:return $this->getBoolean($column['name']);
          case self::NUMERIC:return $this->getNum($column['name']);
          case self::INDEX:return $this->getIndex($column['name'], $column['length']);
          case self::IGNORE_0:return false;
      }
      throw new Exception\InvalidColumnException(sprintf('Cannot handle datatype %s', $column['type']));
  }
  public function getDataLength($column){
      switch ($column['type']) {
          case self::DATE : return 8;
          case self::DATETIME : return 8;
          case self::LOGICAL : return 1;
          default : return $column['length'];
      }
  }
  public function getIndex($columnName, $length){
      $s = $this->choppedData[$columnName];
      if (!$s){
          return false;
      }
      if($this->table->foxpro){
          $su = unpack("i", $s);
          $ret = $su[1];
      }else{
          $ret = ord($s[0]);

          for ($i = 1; $i < $length; $i++) {
              $ret += $i * 256 * ord($s[$i]);
          }
      }
      return $ret;
  }
  public function forceGetString($columnName){
      $data = trim($this->choppedData[$columnName]);
      if ($this->convertFrom) {
          $data = iconv($this->convertFrom, 'utf-8', $data);
      }
      if (!isset($data[0]) || ord($data[0]) === 0) {
          return null;
      }
      return $data;
  }
  private function setFilePos($offset){
      $this->filePos = $offset;
      fseek($this->fp, $this->filePos);
  }
  protected function readString($l){
      return $this->readBytes($l);
  }
  public function getDateTime($columnName){
      $raw = $this->choppedData[$columnName];
      $buf = unpack('i', substr($raw, 0, 4));
      $intdate = $buf[1];
      $buf = unpack('i', substr($raw, 4, 4));
      $inttime = $buf[1];
      if ($intdate == 0 && $inttime == 0) {
          return false;
      }
      $longdate = ($intdate - $this->zerodate) * 86400;
      return $longdate + ($inttime / 1000);
  }
  protected function readShort(){
      $buf = unpack('S', $this->readBytes(2));
      return $buf[1];
  }
  protected function read3ByteDate(){
      $y = unpack('c', $this->readByte());
      $m = unpack('c', $this->readByte());
      $d = unpack('c', $this->readByte());
      return mktime(0, 0, 0, $m[1], $d[1] ,$y[1] > 70 ? 1900 + $y[1] : 2000 + $y[1]);
  }
  protected function readChar(){
      $buf = unpack('C', $this->readBytes(1));
      return $buf[1];
  }
  protected function readBytes($l){
      $this->filePos += $l;
      return fread($this->fp, $l);
  }
  protected function readByte(){
      $this->filePos++;
      return fread($this->fp, 1);
  }
  protected function readInt(){
      $buf = unpack('I', $this->readBytes(4));
      return $buf[1];
  }
  /*
  * DB Functions
  */
  protected function fetchAssoc($query){
    if (!$result = $this->db->query($query)) {
        $this->error_query($query);
    }
    $data = [];
    while ($row = $result->fetch_assoc()) {
        $data[] = $row;
    }
    return $data;
  }
  protected function tableExists($table){
    if ($result = $this->db->query("SHOW TABLES LIKE '".$table."'")) {
      return $result->num_rows == 1 ? true : false;
    }else{
      return false;
    }
  }
  public function errorQuery($query){}
}
