#!/usr/bin/env tclsh

#
# DBF (dBase) file reader/writer for Tcl.
# Author: Pawe³ Salawa (pawelsalawa [at] gmail [dot] com)
#
# Implementation references:
# http://www.clicketyclick.dk/databases/xbase/format/dbf.html
# http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
# http://www.dbf2002.com/dbf-file-format.html
# http://ulisse.elettra.trieste.it/services/doc/dbase/DBFstruct.htm
# http://devzone.advantagedatabase.com/dz/webhelp/advantage9.0/server1/dbf_field_types_and_specifications.htm
#
# Changelog:
# 14.09.2012 - Version 0.5
#            - Fixed reading Visual FoxPro (hex version \x32) of "N" fields.
#            - Fixed [getAllData] method.
# 15.08.2012 - Version 0.4
#            - Added "isFlagShip" method.
#            - Added character encoding support. This comes with methods: setEncoding, encoding and getRecognizedEncodings.
#            - Fixed reading 3-length "V" columns (dates) so they are actually read as "D" column.
#            - Implemented [vacuum] method.
#            - Implemented [tell] method.
#            - Introduced new datetime conversion methods: unixTimeToJulianDate, shortDateToUnixTime, unixTimeToShortDate.
#            - Fixed reading column length from header (for big lengths).
#            - Fixed internal methods: convertShortDate and convertShortDateToBin to support dates before 1970.
#            - Fixed reading "@" columns.
# 09.08.2012 - Version 0.3.
#            - Field type "T" is now read correctly.
#            - Added julianDateToUnixTime function to convert jualian dates after 1970 to unixtime, which is more Tcl-like.
#            - Fixed flushInitialHeader when creating new dbf file in any month except of 4th quater of the year - didn't work at all.
# 01.08.2012 - Version 0.2.
#            - Fixed reading of Visual FoxPro files.
#            - "D" type is now read as string, not as unixtime, cause unixtime doesn't deal with dates before 1970.
#            - "C" type is now trimmed from left side, so there are no extra white spaces before the actual value.
#            - Added pkgIndex.tcl, making a complete Tcl package.
#
# 01.11.2011 - First release: 0.1.

package require Itcl
package provide tdbf 0.5


namespace eval tdbf {

	itcl::class dbf {

		constructor {{errorHandler ""}} {}
		destructor {}

		common _codePageMap [dict create]
		dict set _codePageMap 01 cp437			;# DOS USA
		dict set _codePageMap 02 cp850			;# DOS Multilingual
		dict set _codePageMap 03 cp1252			;# Windows ANSI
		dict set _codePageMap 04 macRoman		;# Standard Macintosh
		dict set _codePageMap 64 cp852			;# EE MS-DOS
		dict set _codePageMap 65 cp865			;# Nordic MS-DOS
		dict set _codePageMap 66 cp866			;# Russian MS-DOS
		dict set _codePageMap 67 cp861			;# Icelandic MS-DOS
		dict set _codePageMap 68 cp895			;# Kamenicky (Czech) MS-DOS
		dict set _codePageMap 69 cp790			;# Mazovia (Polish) MS-DOS
		dict set _codePageMap 6A cp737			;# Greek MS-DOS (437G)
		dict set _codePageMap 6B cp857			;# Turkish MS-DOS
		dict set _codePageMap 78 cp950			;# Chinese Windows
		dict set _codePageMap 7A cp936			;# Chinese Windows
		dict set _codePageMap 7D cp1255			;# Herbew Windows
		dict set _codePageMap 7E cp1256			;# Arabic Windows
		dict set _codePageMap 8B cp932			;# Japanese Windows
		dict set _codePageMap 96 macCyrillic	;# Russian Macintosh
		#dict set _codePageMap 97 cp10029		;# Eastern European Macintosh
		dict set _codePageMap 98 macGreek		;# Greek Macintosh
		dict set _codePageMap C8 cp1250			;# Windows EE
		dict set _codePageMap C9 cp1251			;# Russian Windows
		dict set _codePageMap CA cp1254			;# Turkish Windows
		dict set _codePageMap CB cp1253			;# Greek Windows
		common _validCodeMaps [dict values $_codePageMap]
		# Now create reverse mapping:
		dict for {k v} $_codePageMap {
			dict set _codePageMap $v $k
		}

		private {
			common nullChar "\x00"

			variable _errorHandler ""
			variable _inDestructor 0
			variable _fd ""
			variable _memoFd ""
			variable _encoding ""

			# Writting helpers
			variable _createdFile ""
			variable _modified 0
			variable _fieldsModified 0
			variable _memoBuffer [dict create]
			variable _dbtBlockBeforeCashing ""
			variable _hasHeader 0

			# Different database type flags
			variable _useDecimalAsHighByte 0
			variable _flagShip 0
			variable _expectMemo 0
			variable _useSingleMemoTerminator 0

			# Header values
			variable _version ""
			variable _lastModifiedAt ""
			variable _recordsCount 0 ;# this includes deleted records
			variable _recordSize ""
			variable _offset ""
			variable _versionName ""
			variable _languageDriver "01"
			
			# Dbt header values
			variable _dbtNextAvailableBlock ""
			variable _dbtTerminator "\x1a\x1a"
			
			# Binary format
			variable _binFormat ""
			variable _binFormatList [list]
			variable _binVarList [list]
			variable _binReadPostProcessing [dict create]
			variable _binWritePreProcessing [dict create]
			variable _binEncodingConversion [dict create]

			# List of column dicts
			variable _columns [list]
			variable _columnNames [list]
			variable _columnNamesCached 0

			method tempFile {}
			method prepareRecordOffsets {}
			method readHeader {}
			method applyDbType {}
			method readValue {binData}
			method readValueToArray {binData arrName}
			method buildBinaryFormat {}
			method readMemoValue {pointer}
			method readMemoHeader {}
			method create {file}
			method getColumnByName {name}

			# Methods for writting DBF files
			method createDbt {}
			method flushInitialHeader {}
			method updateHeader {}
			method writeMemoValue {value}
			method flushMemoValues {}
			method rollbackMemoBuffer {}
			method updateFields {}
			method prepareFieldsBinaryData {}
			method prepareBinaryDataToWrite {values {columnName {}}}
			method getFreeRecordAddress {}

			# Converting routines for reading from file
			proc convertCurrency {value}
			proc convertDateTime {value}
			proc convertShortDate {value}
			proc convertTimeStamp {value}
			proc convertNumber {value}
			proc convertCurrencyToBin {value}
			proc convertDateTimeToBin {value}
			proc convertShortDateToBin {value}
			proc convertTimeStampToBin {value}
		}

		public {
			method open {file}
			method read {fd {memoFd ""}}
			method close {}
			method getVersion {}
			method getVersionName {}
			method getLastModificationDate {}
			method getColumns {}
			method getColumnNames {}
			method getColumnType {name {detailed false}}
			method isColumnIndexed {name}
			method getDataCount {}
			method getRecordSize {}
			method getAllData {}
			method for {arrName body}
			method tell {}
			method seek {index}
			method gets {}
			method vacuum {}
			method addColumn {name type {length ""} {precision ""}}
			method insert {values}
			method delete {index}
			method update {index values {columnName ""}}
			method isFlagShip {}
			method setEncoding {encoding}
			method encoding {}
			proc getRecognizedEncodings {}
			proc julianDateToUnixTime {value}
			proc unixTimeToJulianDate {value}
			proc shortDateToUnixTime {value}
			proc unixTimeToShortDate {value}
		}
	}

	itcl::body dbf::constructor {{errorHandler ""}} {
		set _errorHandler $errorHandler
		set _encoding [::encoding system]
	}

	itcl::body dbf::destructor {} {
		set _inDestructor 1
		close
	}

	itcl::body dbf::close {} {
		if {$_memoFd != ""} {
			::seek $_memoFd 0
			puts -nonewline $_memoFd [binary format i $_dbtNextAvailableBlock]
			::close $_memoFd
			set _memoFd ""
		}
		if {$_fd != ""} {
			if {$_hasHeader} {
				if {$_modified} {
					updateHeader
				}
				if {$_fieldsModified} {
					updateFields
				}
			} elseif {!$_hasHeader} {
				flushInitialHeader
			}

			if {$_modified || $_createdFile != ""} {
				::seek $_fd -1 end
				if {[::read $_fd 1] != "\x1a"} {
					puts -nonewline $_fd \x1a
					flush $_fd
				}
			}

			::close $_fd
			set _fd ""
		}
		if {!$_inDestructor} {
			itcl::delete object $this
		}
	}

	itcl::body dbf::open {file} {
		set _dbfFile $file
		if {![file exists $file]} {
			create $file
			return ""
		}

		set _fd [::open $file r+]
		fconfigure $_fd -translation binary -blocking 0

		set dbt [string range $_dbfFile 0 end-[string length [file extension $_dbfFile]]]
		append dbt ".dbt"
		if {[file readable $dbt]} {
			set _memoFd [::open $dbt r+]
			fconfigure $_memoFd -translation binary -blocking 0
		}
		$this read $_fd $_memoFd

		return ""
	}

	itcl::body dbf::read {fd {memoFd ""}} {
		set _fd $fd
		readHeader

		::seek $_fd $_offset

		if {$memoFd != ""} {
			set _memoFd $memoFd
			$this readMemoHeader
		}

		if {$_memoFd == "" && $_expectMemo && $_errorHandler != ""} {
			eval $_errorHandler DBT_DOESNT_EXIST
		}
	}

	itcl::body dbf::readMemoHeader {} {
		set data [::read $_memoFd 4]
		binary scan $data i _dbtNextAvailableBlock
		if {$_useSingleMemoTerminator} {
			set _dbtTerminator "\x1a"
		}
	}

	itcl::body dbf::create {file} {
		set _fd [::open $file w+]
		fconfigure $_fd -translation binary -blocking 0
		set _createdFile $file

		set _version "32"
		set _recordSize 0
		set _recordsCount 0
		applyDbType
	}

	itcl::body dbf::createDbt {} {
		set dbt [string range $_createdFile 0 end-[string length [file extension $_createdFile]]]
		append dbt ".dbt"
		
		if {[file exists $dbt] && ![file writable $dbt] || ![file writable [file dirname $dbt]]} {
			if {$_errorHandler != ""} {
				eval $_errorHandler DBT_READ_ONLY
			}
			return
		}
		
		if {[file exists $dbt]} {
			set mode r+
		} else {
			set mode w+
		}
		
		set _memoFd [::open $dbt $mode]
		fconfigure $_memoFd -translation binary -blocking 0
		puts -nonewline $_memoFd "\x01[string repeat \x00 511]"
		
		set _dbtNextAvailableBlock 1
	}

	itcl::body dbf::addColumn {name type {length ""} {precision ""}} {
		if {$_createdFile == "" && $_recordsCount > 0} {
			if {$_errorHandler != ""} {
				eval $_errorHandler RECORDS_EXIST \$name
			}
			return
		}
		foreach col $_columns {
			if {[dict get $col name] == $name} {
				if {$_errorHandler != ""} {
					eval $_errorHandler COLUMN_EXISTS \$name
				}
				return
			}
		}

		if {[string length $name] > 10 && $_errorHandler != ""} {
			eval $_errorHandler COLUMN_NAME_TOO_LONG \$name
		}

		if {$precision == ""} {
			set precision 0
		}

		switch -- $type {
			"N" {
				if {$length == ""} {
					error "Length parameter is required for $type type."
				}
				if {$length > 20} {
					error "Max length for N type is 20."
				}
			}
			"C" {
				if {$length == ""} {
					error "Length parameter is required for $type type."
				}
				if {$length > 64*1024} {
					error "Max length for C type is 64KB."
				}
			}
			"L" {
				set length 1
				set precision 0
			}
			"D" {
				set length 8
				set precision 0
			}
			"M" - "B" - "G" - "P" {
				set length 10
				set precision 0
			}
			"F" {
				set length 20
			}
			"Y" {
				set length 8
				set precision 4
			}
			"T" {
				set length 8
				set precision 0
			}
			"I" - "+" {
				set length 4
				set precision 0
			}
			"V" - "X" {
				error "$type is not supported by DBF writter."
			}
			"@" {
				set length 8
				set precision 0
			}
			"O" {
				set length 8
			}
			default {
				error "$type is not supported by DBF writter."
			}
		}

		lappend _columns [dict create name $name type $type length $length precision $precision indexed 0]
		set _fieldsModified 1
		set _columnNamesCached 0
	}


	itcl::body dbf::getColumnType {name {detailed false}} {
		set col [getColumnByName $name]
		if {$col == ""} {
			error "No column named: $name"
		}
		if {$detailed} {
			return [dict remove $col name indexed]
		} else {
			return [dict get $col type]
		}
	}

	itcl::body dbf::getColumnByName {name} {
		foreach col $_columns {
			if {![string equal -nocase $name [dict get $col name]]} continue
			return $col
		}
		return ""
	}

	itcl::body dbf::isColumnIndexed {name} {
		set col [getColumnByName $name]
		if {$col == ""} {
			error "No column named: $name"
		}
		return [dict get $col indexed]
	}

	itcl::body dbf::setEncoding {encoding} {
		if {$encoding in [::encoding names]} {
			set _encoding $encoding
			if {[dict exists $_codePageMap $encoding]} {
				set _languageDriver [dict get $_codePageMap $encoding]
			}
		}
	}

	itcl::body dbf::encoding {} {
		return $_encoding
	}

	itcl::body dbf::getRecognizedEncodings {} {
		return $_validCodeMaps
	}

	itcl::body dbf::convertCurrency {value} {
		return [string range $value 0 end-4].[string range $value end-3 end]
	}

	itcl::body dbf::convertDateTime {value} {
		return $value
	}

	itcl::body dbf::convertShortDate {value} {
		lassign $value yearAdd month day
		set year [expr {1900 + $yearAdd}]
		#clock scan "$day $month $year" -format "%d %m %Y"
		if {$month < 10} {
			set month "0$month"
		}
		if {$day < 10} {
			set day "0$day"
		}
		return "$year$month$day"
	}

	itcl::body dbf::convertTimeStamp {value} {
		# TODO: write code to format date in similar way as for convertDateTime
		return $value
	}

	itcl::body dbf::convertCurrencyToBin {value} {
		string replace $value end-4 end-4
	}

	itcl::body dbf::convertDateTimeToBin {value} {
		#lassign $value julianDay julianTime
		return $value
	}

	itcl::body dbf::convertShortDateToBin {value} {
		#lassign $value day month year
		set year [string range $value 0 3]
		set month [string range $value 4 5]
		set day [string range $value 6 7]
		set yearAdd [expr {$year - 1900}]
		if {$yearAdd > 0} {
			set yearAdd [string trimleft $yearAdd 0]
		}
		list $yearAdd [string trimleft $month 0] [string trimleft $day 0]
	}

	itcl::body dbf::convertTimeStampToBin {value} {
		# TODO: implement when [convertTimeStamp] is done.
		return $value
	}

	itcl::body dbf::convertNumber {value} {
		if {$value != ""} {
			expr {$value}
		} else {
			return ""
		}
	}
	
	itcl::body dbf::julianDateToUnixTime {value} {
		lassign $value julianDay julianTime
		if {$julianDay <= 2440587} {
			# No UnixTime before 1970
			return 0
		}
		expr {int(round( ($julianDay + (double($julianTime) / 86400000) - 2440587.5) * 86400 ))}
	}

	itcl::body dbf::unixTimeToJulianDate {value} {
		set julianDate [expr {double($value) / 86400 + 2440587.5}]
		set days [expr {int($julianDate)}]
		set msecs [expr {int(($julianDate - int($julianDate)) * 86400000) % 86400000}]
		list $days $msecs
	}

	itcl::body dbf::shortDateToUnixTime {value} {
		set year [string range $value 0 3]
		set month [string range $value 4 5]
		set day [string range $value 6 7]
		if {$year < 1970} {
			return 0
		}
		clock scan "$day $month $year" -format "%d %m %Y"
	}

	itcl::body dbf::unixTimeToShortDate {value} {
		clock format $value -format "%Y%m%d"
	}

	itcl::body dbf::applyDbType {} {
		switch -glob -- [string toupper $_version] {
			"?4" - "?C" {
				# dBASE 7
				set _versionName "dBASE 7"
			}
			"02" {
				# FoxBASE
				set _versionName "FoxBASE"
			}
			"03" {
				# FoxBASE+/Dbase III plus, no memo
				set _versionName "FoxBASE+/dBASE III plus"
			}
			"05" {
				# dBASE V w/o memo file
				set _versionName "dBASE V"
			}
			"07" {
				# VISUAL OBJECTS (first 1.0 versions) for the Dbase III files w/o memo file
				set _versionName "dBASE III"
			}
			"30" {
				# Visual FoxPro
				set _versionName "Visual FoxPro"
				set _useDecimalAsHighByte 0
				set _useSingleMemoTerminator 1
			}
			"31" {
				# Visual FoxPro, autoincrement enabled
				set _versionName "Visual FoxPro"
				set _useDecimalAsHighByte 1
				set _useSingleMemoTerminator 1
			}
			"32" {
				# Visual FoxPro with field type Varchar or Varbinary
				set _versionName "Visual FoxPro"
				set _useDecimalAsHighByte 0
				set _useSingleMemoTerminator 1
			}
			"43" {
				# dBASE IV SQL table files, no memo.
				# Also .dbv memo var size (Flagship)
				set _versionName "dBASE IV"
				set _flagShip 1
			}
			"63" {
				# dBASE IV SQL system files, no memo
				set _versionName "dBASE IV"
			}
			"7B" {
				# dBASE IV with memo
				set _versionName "dBASE IV"
				set _expectMemo 1
			}
			"83" {
				# FoxBASE+/dBASE III PLUS, with memo
				set _versionName "FoxBASE+/dBASE III plus"
				set _expectMemo 1
			}
			"87" {
				# VISUAL OBJECTS (first 1.0 versions) for the Dbase III files
				# (NTX clipper driver) with memo file
				set _versionName "dBASE III"
				set _useDecimalAsHighByte 1
				set _expectMemo 1
			}
			"8B" {
				# dBASE IV with memo
				set _versionName "dBASE IV"
				set _expectMemo 1
			}
			"8E" {
				# dBASE IV w. SQL table
				set _versionName "dBASE IV"
				set _expectMemo 1
			}
			"B3" {
				# .dbv and .dbt memo (Flagship)
				set _versionName "dBASE memo"
				set _flagShip 1
			}
			"CB" {
				# dBASE IV SQL table files, with memo
				set _versionName "dBASE IV"
				set _expectMemo 1
			}
			"E5" {
				# Clipper SIX driver w. SMT memo file.
				set _versionName "Clipper SIX"
				set _useDecimalAsHighByte 1
				set _expectMemo 1
			}
			"F5" {
				# FoxPro 2.x (or earlier) with memo
				set _versionName "FoxPro 2.x"
				set _useDecimalAsHighByte 1
				set _expectMemo 1
				set _useSingleMemoTerminator 1
			}
			"FB" {
				# FoxBASE
				set _versionName "FoxBASE"
				set _expectMemo 1
			}
			default {
				set _versionName "unknown"
			}
		}
	}

	itcl::body dbf::readHeader {} {
		set head [::read $_fd 32]
		if {[string length $head] != 32} return
		binary scan $head H2c3issx2ccx12cH2 _version lastMod _recordsCount headerSize _recordSize \
			incompleteTransaction encryptionFlag mdxFlag _languageDriver

		# Last modified
		lassign $lastMod yearAdd month day
		set year [expr {1900 + $yearAdd}]
		set _lastModifiedAt [clock scan "$day $month $year" -format "%d %m %Y"]

		set _offset $headerSize

		applyDbType

		set fieldTerminator "\x0d"
		while {true} {
			# Reading 1 byte and checking if this is end of fields
			set data [::read $_fd 1]
			if {$data == $fieldTerminator || [eof $_fd]} {
				break
			}

			# Not an end. Reading rest of 32 bytes.
			append data [::read $_fd 31]

			# Parse field description
			binary scan $data A10x1ax4cucux13c fieldName fieldType fieldLength decimalCount indexFlag
			set col [dict create name $fieldName type $fieldType indexed $indexFlag]

			# Some database types use decimalCount field as hi-byte for length of N and I types.
			if {$_useDecimalAsHighByte && $fieldType in [list "N" "I"]} {
				dict set col length [expr {$decimalCount * 256 + $fieldLength}]
				dict set col precision 0
			} else {
				dict set col length $fieldLength
				dict set col precision $decimalCount
			}
			
			lappend _columns $col
		}

		set _hasHeader 1

		set _languageDriver [string toupper $_languageDriver]
		if {[dict exists $_codePageMap $_languageDriver]} {
			set _encoding [dict get $_codePageMap $_languageDriver]
		}

		buildBinaryFormat
	}

	itcl::body dbf::flushInitialHeader {} {
		set _lastModifiedAt [clock seconds]
		set modDate [convertShortDateToBin [unixTimeToShortDate $_lastModifiedAt]]

		set fieldsBinData [prepareFieldsBinaryData]

		# Write header
		::seek $_fd 0
		puts -nonewline $_fd [binary format H2c3issx17H2x2 $_version $modDate 0 $_offset $_recordSize $_languageDriver]
		puts -nonewline $_fd $fieldsBinData
		flush $_fd
		
		set _hasHeader 1

		buildBinaryFormat
	}

	itcl::body dbf::prepareFieldsBinaryData {} {
		set _recordSize 0
		set _offset 32
		set fieldsBinData ""
		foreach col $_columns {
			incr _recordSize [dict get $col length]
			incr _offset 32

			# Prepare per-field binary data
			set name [dict get $col name]
			set type [dict get $col type]
			set length [dict get $col length]
			set precision [dict get $col precision]
			if {$length > 255} {
				set precision [expr {$length / 256}]
				set length [expr {$length % 256}]
			}

			append fieldsBinData [binary format a10xax4ccx13c $name $type $length $precision 0]
		}

		# Deletion flag
		incr _recordSize

		# Field list termination marker
		append fieldsBinData \x0d
		incr _offset 1

		return $fieldsBinData
	}

	itcl::body dbf::updateHeader {} {
		# Modification date
		set _lastModifiedAt [clock seconds]
		set modDate [convertShortDateToBin $_lastModifiedAt]
		
		::seek $_fd 1
		puts -nonewline $_fd [binary format c3 $modDate]

		# Recourd count
		::seek $_fd 4
		puts -nonewline $_fd [binary format i $_recordsCount]
		flush $_fd
	}

	itcl::body dbf::updateFields {} {
		::seek $_fd 32
		set fieldsBinData [prepareFieldsBinaryData]
		puts -nonewline $_fd $fieldsBinData
		flush $_fd
	}

	itcl::body dbf::insert {values} {
		if {[llength $values] != [llength $_columns]} {
			error "Expected [llength $_columns] values, but got [llength $values]."
		}

		if {!$_hasHeader} {
			flushInitialHeader
		}

		set binData [prepareBinaryDataToWrite $values]

		::seek $_fd [getFreeRecordAddress]
		puts -nonewline $_fd $binData
		flush $_fd
		flushMemoValues

		incr _recordsCount
		set _modified 1
		return ""
	}

	itcl::body dbf::getFreeRecordAddress {} {
		set sizeMinOne [expr {$_recordSize - 1}]
		::seek $_fd $_offset
		::for {set i 0} {$i < $_recordsCount} {incr i} {
			if {[::read $_fd 1] == "\x2a"} {
				return [expr {[tell $_fd] - 1}]
			}
			::seek $_fd $sizeMinOne current
		}
		return [expr {$_offset + $_recordsCount * $_recordSize}]
	}

	itcl::body dbf::prepareBinaryDataToWrite {values {columnName {}}} {
		if {$columnName != ""} {
			#
			# Single value
			#
			set columnNames [getColumnNames]
			if {$columnName ni $columnNames} {
				error "Unknown column name: $columnName"
			}
			set columnIdx [lsearch -exact $columnNames $columnName]
			set value $values

			set code [dict get $_binWritePreProcessing $columnName]
			if {$code != ""} {
				set convertedValue [eval $code]
			} else {
				set convertedValue $value
			}

			if {[dict get $_binEncodingConversion $columnName] && $_encoding != [::encoding system]} {
				set convertedValue [::encoding convertto $_encoding $convertedValue]
			}

			set binData \x20 ;# not deleted
			append binData [binary format [lindex $_binFormatList $columnIdx] $convertedValue]
		} else {
			#
			# Multiple values
			#
			set convertedValues [list]
			foreach value $values col $_columns {
				set columnName [dict get $col name]
				set code [dict get $_binWritePreProcessing $columnName]
				if {$code != ""} {
					set convertedValue [eval $code]
				} else {
					set convertedValue $value
				}

				if {[dict get $_binEncodingConversion $columnName] && $_encoding != [::encoding system]} {
					puts "$convertedValue -> [::encoding convertto $_encoding $convertedValue]"
					set convertedValue [::encoding convertto $_encoding $convertedValue]
				}

				lappend convertedValues $convertedValue
			}

			# If provided values are not correct, we need to be safe
			if {[catch {
				set binData \x20 ;# not deleted
				append binData [binary format $_binFormat {*}$convertedValues]
			} err]} {
				# In this case memo values are not written, just dropped.
				rollbackMemoBuffer
				error $err
			}
		}
		return $binData
	}

	itcl::body dbf::update {index values {columnName {}}} {
		if {[llength $values] != [llength $_columns] && $columnName == ""} {
			error "Expected [llength $_columns] values, but got [llength $values]."
		}

		if {$_recordsCount == 0} {
			if {$_errorHandler != ""} {
				eval $_errorHandler NO_RECORDS_WHILE_UPDATING
			}
			return false
		}

		if {![seek $index]} {
			return false
		}

		if {$columnName != ""} {
			#
			# Updating single field
			#
			set binData [prepareBinaryDataToWrite $values $columnName]

			# Moving to specified field
			foreach col $_columns {
				if {[dict get $col name] != $columnName} {
					# Skip this field
					::seek $_fd [dict get $col length] current
				} else {
					# Found it!
					break
				}
			}

			# Writting bytes
			puts -nonewline $_fd $binData
			flush $_fd
			flushMemoValues

			set _modified 1
			
		} else {
			#
			# Updating entire record
			#
			set binData [prepareBinaryDataToWrite $values]
			puts -nonewline $_fd $binData
			flush $_fd
			flushMemoValues

			set _modified 1
		}
		return true
	}

	itcl::body dbf::delete {index} {
		if {![seek $index]} {
			return false
		}
		puts -nonewline $_fd \x2a
		flush $_fd
		return true
	}

	itcl::body dbf::getVersion {} {
		return $_version
	}

	itcl::body dbf::getVersionName {} {
		return $_versionName
	}

	itcl::body dbf::getLastModificationDate {} {
		return $_lastModifiedAt
	}

	itcl::body dbf::getColumns {} {
		return $_columns
	}

	itcl::body dbf::getColumnNames {} {
		if {$_columnNamesCached} {
			return $_columnNames
		}

		# Not cached. Prepare list and cache it.
		set _columnNames [list]
		foreach col $_columns {
			lappend _columnNames [dict get $col name]
		}
		set _columnNamesCached 1
		return $_columnNames
	}

	itcl::body dbf::getDataCount {} {
		if {$_offset == "" || $_offset < 32} {
			return 0
		}

		set count 0
		set sizeMinOne [expr {$_recordSize - 1}]
		::seek $_fd $_offset
		::for {set i 0} {$i < $_recordsCount && ![eof $_fd]} {incr i} {
			if {[::read $_fd 1] != "\x2a"} {
				incr count
			}
			::seek $_fd $sizeMinOne current
		}
		return $count
	}

	itcl::body dbf::readMemoValue {pointer} {
		if {$_memoFd == ""} {
			return ""
		}

		::seek $_memoFd [expr {$pointer * 512}]
		set data ""
		set newData ""
		while {[string first $_dbtTerminator $newData] == -1 && ![eof $_memoFd]} {
			set newData [::read $_memoFd 512]
			append data $newData
		}
		set idx [string first $_dbtTerminator $data]
		incr idx -1
		return [string range $data 0 $idx]
	}

	itcl::body dbf::writeMemoValue {value} {
		if {$_memoFd == ""} {
			createDbt
			if {$_memoFd == ""} {
				return ""
			}
		}

		# Termination includes in value
		append value "\x1a\x1a"
		
		# Count how many 512-byte blocks we need
		set length [string length $value]
		set blocks [expr {$length / 512}]
		if {$length % 512 > 0} {
			incr blocks
		}

		# Remember block in case of rolling back
		if {$_dbtBlockBeforeCashing == ""} {
			set _dbtBlockBeforeCashing $_dbtNextAvailableBlock
		}

		# Put memo into the buffer at current available block address.
		set blockPtr $_dbtNextAvailableBlock
		dict set _memoBuffer $blockPtr $value

		# Increment available address.
		incr _dbtNextAvailableBlock $blocks

		# Return cached pointer.
		return $blockPtr
	}

	itcl::body dbf::flushMemoValues {} {
		if {$_memoFd == ""} {
			return
		}

		# Flush in ascending pointers order
		foreach ptr [lsort -dictionary [dict keys $_memoBuffer]] {
			set byteAddr [expr {$ptr * 512}]
			::seek $_memoFd $byteAddr
			set realAddr [tell $_memoFd]
			if {$realAddr < $byteAddr} {
				puts -nonewline $_memoFd [string repeat \x00 [expr {$byteAddr - $realAddr}]]
			}

			puts -nonewline $_memoFd [dict get $_memoBuffer $ptr]
		}
		flush $_memoFd

		set _memoBuffer [dict create]
	}

	itcl::body dbf::rollbackMemoBuffer {} {
		if {$_dbtBlockBeforeCashing != ""} {
			set _dbtNextAvailableBlock $_dbtBlockBeforeCashing
			set _dbtBlockBeforeCashing ""
		}
		set _memoBuffer [dict create]
	}

	itcl::body dbf::readValue {binData} {
		set vals [list]
		
		# Scan binary stream using format built by [buildBinaryFormat]
		binary scan $binData $_binFormat {*}$_binVarList

		# Go through all data read and apply special conversion routines to values
		foreach col [getColumnNames] {
			set code [dict get $_binReadPostProcessing $col]
			set value $field($col) ;# necessary for post-read routines
			if {$code != ""} {
				set field($col) [eval $code]
			}
			if {[dict get $_binEncodingConversion $col] && $_encoding != [::encoding system]} {
				set field($col) [::encoding convertfrom $_encoding $field($col)]
			}
			lappend vals $field($col)
		}
		return $vals
	}

	itcl::body dbf::readValueToArray {binData arrName} {
		upvar $arrName field

		# This method seems to copy much from [readValue], but this (and [readValue]) is key method
		# to read data and it has to be as fast as possible. Skipping unecessary condition
		# checks is a good idea here. That's why there're two similar methods.

		# Scan binary stream using format built by [buildBinaryFormat]
		binary scan $binData $_binFormat {*}$_binVarList

		# Go through all data read and apply special conversion routines to values
		foreach col [array names field] {
			set code [dict get $_binReadPostProcessing $col]
			set value $field($col) ;# necessary for post-read routines
			if {$code != ""} {
				set field($col) [eval $code]
			}
			if {[dict get $_binEncodingConversion $col] && $_encoding != [::encoding system]} {
				set field($col) [::encoding convertfrom $_encoding $field($col)]
			}
		}
	}

	itcl::body dbf::buildBinaryFormat {} {
		set binFormat [list]
		foreach field $_columns {
			set name [dict get $field name]
			lappend _binVarList "field($name)"

			dict set _binReadPostProcessing $name {}
			dict set _binWritePreProcessing $name {}
			dict set _binEncodingConversion $name 0

			set type [dict get $field type]
			set length [dict get $field length]
			switch -- $type {
				"C" {
					# ASCII text
					#
					# Return as string.
					lappend binFormat A$length

					dict set _binReadPostProcessing $name {
						string trimleft $value
					}

					dict set _binEncodingConversion $name 1
				}
				"N" {
					# ASCII text (include sign and decimal point). Valid characters: "0" - "9" and "-".
					#
					# Return as number.
					lappend binFormat A$length

					dict set _binReadPostProcessing $name {
						convertNumber $value
					}
				}
				"L" {
					# Boolean/byte (8 bit) Legal values: 
					# ?	 	Not initialised (default)
					# Y,y	Yes
					# N,n	No
					# F,f	False
					# T,t	True
					# space Not initialised
					#
					# Return as boolean or empty string.
					lappend binFormat A$length

					dict set _binReadPostProcessing $name {
						expr {$value in [list Y y T t] ? true : $value in [list N n F f] ? false : ""}
					}
					dict set _binWritePreProcessing $name {
						expr {$value != "" ? ($value ? "T" : "F") : "?"}
					}
				}
				"I" - "+" {
					# Integer. 4 bytes. Little-endian. Leftmost bit used to indicate sign, 0 negative.
					#
					# Return as number (integer).
					lappend binFormat i
				}
				"D" {
					# Date in format YYYYMMDD. A date like 0000-00- 00 is *NOT* valid.
					#
					# Return as [clock seconds] format.
					lappend binFormat A8
				}
				"M" - "G" {
					# Pointer to ASCII text field in memo file 10 digits representing a pointer
					# to a DBT block (default is blanks). 
					#
					# Return as pointer integer format.
					lappend binFormat A$length

					dict set _binReadPostProcessing $name {
						readMemoValue [convertNumber $value]
					}
					dict set _binWritePreProcessing $name {
						writeMemoValue $value
					}

					dict set _binEncodingConversion $name 1
				}
				"F" {
					# 20 digits
					# Number stored as a string, right justified, and padded with blanks to the width of the field
					#
					# Return as number (float) format.
					lappend binFormat A$length

					dict set _binReadPostProcessing $name {
						convertNumber $value
					}
				}
				"B" - "P" {
					# 10 digits representing a .DBT block number.
					# The number is stored as a string, right justified and padded with blanks.
					#
					# Return as pointer integer format.
					lappend binFormat A$length

					dict set _binReadPostProcessing $name {
						readMemoValue [convertNumber $value]
					}
					dict set _binWritePreProcessing $name {
						writeMemoValue $value
					}
				}
				"O" {
					# 8 bytes - no conversions, stored as a double.
					#
					# Return as number (double) format.
					lappend binFormat q
				}
				"Y" {
					# Currency in FoxPro.
					# Currency data stored internally as a 64-bit integer, with 4 implied decimal digits.
					#
					# Return as string (out of double range) format.
					lappend binFormat w

					dict set _binReadPostProcessing $name {
						convertCurrency $value
					}
					dict set _binWritePreProcessing $name {
						convertCurrencyToBin $value
					}
				}
				"T" - "@" {
					# DateTime in FoxPro.
					# Stored as 2 unsigned integers.
					#
					# Return as {days milliseconds} of Julian Day format.
					lappend binFormat iu2

					dict set _binReadPostProcessing $name {
						convertDateTime $value
					}
					dict set _binWritePreProcessing $name {
						convertDateTimeToBin $value
					}
				}
				"V" - "X" {
					# VariField. 2-10 bytes
					switch -glob -- "$_flagShip,$length" {
						"1,2" {
							# Short int
							#
							# Return as number (integer) format.
							lappend binFormat s
						}
						"*,3" {
							# Date
							#
							# Return as [clock seconds] format.
							lappend binFormat c3

							dict set _binReadPostProcessing $name {
								convertShortDate $value
							}
							dict set _binWritePreProcessing $name {
								convertShortDateToBin $value
							}
						}
						"*,4" {
							# Long int
							#
							# Return as number (integer) format.
							lappend binFormat i
						}
						"1,8" {
							# Double
							#
							# Return as number (double) format.
							lappend binFormat q
						}
						"1,10" {
							# Memo
							# TODO: this case requires special handling.
							# See:
							# http://www.clicketyclick.dk/databases/xbase/format/data_types.html
							#
							# Return as string.
							lappend binFormat A10

							dict set _binEncodingConversion $name 1
						}
						default {
							# VariChar
							# TODO: need to know how to read 6-bytes pointer to memo
							#       to handle this case. For now return string.
							#
							# Return as string.
							lappend binFormat A$length

							dict set _binEncodingConversion $name 1
						}
					}
				}
			}
		}
		set _binFormatList $binFormat
		set _binFormat [join $binFormat {}]
	}

	itcl::body dbf::getAllData {} {
		set values [list]
		::seek $_fd $_offset
		::for {set i 0} {$i < $_recordsCount} {incr i} {
			set recordData [::read $_fd $_recordSize]
			if {[string index $recordData 0] == "\x2a"} {
				# Record deleted
				continue
			}

			lappend values [readValue [string range $recordData 1 end]]
		}
		return $values
	}

	itcl::body dbf::for {arrName body} {
		upvar $arrName customArray

		::seek $_fd $_offset
		::for {set i 0} {$i < $_recordsCount} {incr i} {
			set recordData [::read $_fd $_recordSize]
			if {[string index $recordData 0] == "\x2a"} {
				# Record deleted
				continue
			}

			readValueToArray [string range $recordData 1 end] customArray
			uplevel $body
		}
	}

	itcl::body dbf::getRecordSize {} {
		return $_recordSize
	}

	itcl::body dbf::prepareRecordOffsets {} {
		if {$_fd == ""} {
			return [list]
		}

		set offset $_offset
		set records [list]
		
		# Prepare list of valid record addresses
		set oldPos [::tell $_fd]
		::seek $_fd $offset
		::for {set i 0} {$i < $_recordsCount} {incr i} {
			if {[::read $_fd 1] != "\x2a"} {
				lappend records $offset
			}
			incr offset $_recordSize
			::seek $_fd $offset
		}
		::seek $_fd $oldPos
		return $records
	}
	
	itcl::body dbf::seek {index} {
		set records [prepareRecordOffsets]
		if {[llength $records] == 0} return

		# Pick requested address
		set offset [lindex $records $index]
		if {$offset != ""} {
			::seek $_fd $offset
			return true
		} else {
			return false
		}
	}

	itcl::body dbf::tell {} {
		set records [prepareRecordOffsets]
		if {[llength $records] == 0} return

		lsearch -exact $records [::tell $_fd]
	}

	itcl::body dbf::gets {} {
		if {$_fd == ""} {
			return ""
		}
		if {[eof $_fd]} {
			return ""
		}

		set recordData [::read $_fd $_recordSize]
		if {[string length $recordData] < $_recordSize} {
			return ""
		}

		# Look for next not deleted record
		set sizeMinOne [expr {$_recordSize - 1}]
		::while {![eof $_fd]} {
			if {[::read $_fd 1] != "\x2a"} {
				::seek $_fd -1 current
				break
			}
			::seek $_fd $sizeMinOne current
		}

		return [readValue [string range $recordData 1 end]]
	}

	itcl::body dbf::isFlagShip {} {
		return $_flagShip
	}

	itcl::body dbf::tempFile {} {
		# Code from http://wiki.tcl.tk/772
		# by Igor Volobouev
		set prefix "dbf_vacuum_"
		set suffix ""
		set chars "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		set nrand_chars 10
		set maxtries 10
		set access [list RDWR CREAT EXCL TRUNC]
		set permission 0600
		set channel ""
		set checked_dir_writable 0
		set mypid [pid]
		::for {set i 0} {$i < $maxtries} {incr i} {
			set newname $prefix
			::for {set j 0} {$j < $nrand_chars} {incr j} {
				append newname [string index $chars \
						[expr ([clock clicks] ^ $mypid) % 62]]
			}
			append newname $suffix
			if {[file exists $newname]} {
				after 1
			} else {
				if {[catch {::open $newname $access $permission} channel]} {
					if {!$checked_dir_writable} {
						set dirname [file dirname $newname]
						if {![file writable $dirname]} {
							error "Directory $dirname is not writable"
						}
						set checked_dir_writable 1
					}
				} else {
					# Success
					::close $channel
					file delete -force $newname
					return $newname
				}
			}
		}
		if {[string compare $channel ""]} {
			error "Failed to open a temporary file: $channel"
		} else {
			error "Failed to find an unused temporary file name"
		}
	}

	itcl::body dbf::vacuum {} {
		package require Tcl 8.5 ;# for [chan truncate]

		set fileName [tempFile]
		set dbfName $fileName.dbf
		set dbtFile $fileName.dbt

		set error 0
		if {[catch {
			tdbf::dbf tempDbf
			tempDbf open $dbfName

			# Copy columns
			foreach col [$this getColumns] {
				tempDbf addColumn [dict get $col name] [dict get $col type] [dict get $col length] [dict get $col precision]
			}

			# Copy all valid rows
			while {[set values [$this gets]] != ""} {
				tempDbf insert $values
			}

			# Done
			itcl::delete object tempDbf
			
			# Now lets copy new contents to original file
			foreach {file fd} [list \
				$dbfName $_fd \
				$dbtFile $_memoFd \
			] {
				if {$fd == ""} continue
				set newFd [::open $file r]
				chan configure $newFd -translation binary
				chan seek $fd 0
				chan truncate $fd 0
				chan copy $newFd $fd
				::close $newFd
			}

			# Go to first record
			$this seek 0
		}]} {
			set err $::errorInfo
			set error 1
		}

		catch {file delete -force $dbfFile}
		catch {file delete -force $dbtFile}
		if {$error} {
			error $err
		}
	}

}
