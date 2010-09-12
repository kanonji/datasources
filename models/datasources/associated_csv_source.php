<?php
/**
 * Comma Separated Values Datasource with association
 *
 * PHP 5
 *
 * CakePHP(tm) : Rapid Development Framework (http://cakephp.org)
 * Copyright 2005-2009, Cake Software Foundation, Inc. (http://cakefoundation.org)
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright     Copyright 2005-2009, Cake Software Foundation, Inc. (http://cakefoundation.org)
 * @link          http://cakephp.org CakePHP(tm) Project
 * @package       datasources
 * @subpackage    datasources.models.datasources
 * @since         CakePHP Datasources v 0.3
 * @license       MIT License (http://www.opensource.org/licenses/mit-license.php)
 *
 * A CakePHP datasource for interacting with files using comma separated value storage.
 *
 * Create a datasource in your config/database.php
 *   public $csvfile = array(
 *     'datasource' => 'Datasources.AssociatedCsvSource',
 *     'path' => '/path/to/file', // Path
 *     'extension' => 'csv', // File extension
 *     'readonly' => true, // Mark for read only access
 *     'recursive' => false // Only false is supported at the moment
 *   );
 */

/**
 * AssociatedCsvSource Datasource
 *
 * @package datasources
 * @subpackage datasources.models.datasources
 */
class AssociatedCsvSource extends CsvSource {

/**
 * Description
 *
 * @var string
 * @access public
 */
	var $description = 'CSV Data Source with association';

/**
 * Column delimiter
 *
 * @var string
 * @access public
 */
	var $delimiter = ';';

/**
 * Maximum Columns
 *
 * @var integer
 * @access public
 */
	var $maxCol = 0;

/**
 * Field Names
 *
 * @var mixed
 * @access public
 */
	var $fields = null;

/**
 * File Handle
 *
 * @var mixed
 * @access public
 */
	var $handle = false;

/**
 * Page to start on
 *
 * @var integer
 * @access public
 */
	var $page = 1;

/**
 * Limit of records
 *
 * @var integer
 * @access public
 */
	var $limit = 99999;

/**
 * Default configuration.
 *
 * @var array
 * @access private
 */
	var $__baseConfig = array(
		'datasource' => 'csv',
		'path' => '.',
		'extension' => 'csv',
		'readonly' => true,
		'recursive' => false);

/**
 * Read Data
 *
 * @param Model $model
 * @param array $queryData
 * @param integer $recursive Number of levels of association
 * @return mixed
 */
	function read(&$model, $queryData = array(), $recursive = null) {
		$config = $this->config;
		$filename = $config['path'] . DS . $model->table . '.' . $config['extension'];
		if (!Set::extract($this->handle, $model->table)) {
			$this->handle[$model->table] = fopen($filename, 'r');
		} else {
			fseek($this->handle[$model->table], 0, SEEK_SET) ;
		}
		$queryData = $this->__scrubQueryData($queryData);

		if (isset($queryData['limit']) && !empty($queryData['limit'])) {
			$this->limit = $queryData['limit'];
		}

		if (isset($queryData['page']) && !empty($queryData['page'])) {
			$this->page = $queryData['page'];
		}

		if (empty($queryData['fields'])) {
			$fields = $this->fields;
			$allFields = true;
		} else {
			$fields = $queryData['fields'];
			$allFields = false;
			$_fieldIndex = array();
			$index = 0;
			// generate an index array of all wanted fields
			foreach($this->fields as $field) {
				if (in_array($field,  $fields)) {
					$_fieldIndex[] = $index;
				}
				$index++;
			}
		}

		$lineCount = 0;
		$recordCount = 0;
		$findCount = 0;
		$resultSet = array();

		// Daten werden aus der Datei in ein Array $data gelesen
		while (($data = fgetcsv($this->handle[$model->table], 8192, $this->delimiter)) !== FALSE) {
			if ($lineCount == 0) {
				$lineCount++;
				continue;
			} else {
				// Skip over records, that are not complete
				if (count($data) < $this->maxCol) {
					$lineCount++;
					continue;
				}

				$record = array();
				$i = 0;
				$record['id'] = $lineCount;
				foreach($this->fields as $field) {
					$field = trim($field, '"\'');
					$item = trim($data[$i++], '"\'');
					if(isset($record[$field]) && is_array($record[$field])){
						$record[$field][] = $item;
					} elseif(isset($record[$field])) {
						$record[$field] = array($record[$field], $item);
					} else{
						$record[$field] = $item;
					}
				}

				if ($this->__checkConditions($record, $queryData['conditions'])) {
					// Compute the virtual pagenumber
					$_page = floor($findCount / $this->limit) + 1;
					$lineCount++;
					if ($this->page <= $_page) {
						if (!$allFields) {
							$record = array();
							$record['id'] = $lineCount;
							if (count($_fieldIndex) > 0) {
								foreach($_fieldIndex as $i) {
									$record[$this->fields[$i]] = trim($data[$i], '"\'');
								}
							}
						}
						$resultSet[] = $record ;
						$recordCount++;
					}
				}
				unset($record);
				$findCount++;

				if ($recordCount >= $this->limit) {
					break;
				}
			}
		}

		if ($model->findQueryType === 'count') {
			return array(array(array('count' => count($resultSet))));
		} else {
			return $resultSet;
		}
	}

/**
 * Private helper method to remove query metadata in given data array.
 *
 * @param array $data Data
 * @return array Cleaned Data
 * @access private
 */
	function __scrubQueryData($data) {
		foreach (array('conditions', 'fields', 'joins', 'order', 'limit', 'offset', 'group') as $key) {
			if (!isset($data[$key]) || empty($data[$key])) {
				$data[$key] = array();
			}
		}
		return $data;
	}

/**
 * Private helper method to check conditions.
 *
 * @param array $record
 * @param array $conditions
 * @return bool
 * @access private
 */
	function __checkConditions($record, $conditions) {
		$result = true;
		foreach ($conditions as $name => $value) {
			if (strtolower($name) === 'or') {
				$cond = $value;
				$result = false;
				foreach ($cond as $name => $value) {
					if (Set::matches($this->__createRule($name, $value), $record)) {
						return true;
					}
				}
			} else {
				if (!Set::matches($this->__createRule($name, $value), $record)) {
					return false;
				}
			}
		}
		return $result;
	}
}