# tdbf
DBF (dBase) file reader/writer for Tcl

## Implementation references:
- http://www.clicketyclick.dk/databases/xbase/format/dbf.html
- http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm
- http://www.dbf2002.com/dbf-file-format.html
- http://ulisse.elettra.trieste.it/services/doc/dbase/DBFstruct.htm
- http://devzone.advantagedatabase.com/dz/webhelp/advantage9.0/server1/dbf_field_types_and_specifications.htm

## Changelog:

#### 14.09.2012
- Version 0.5
- Fixed reading Visual FoxPro (hex version \x32) of "N" fields.
- Fixed [getAllData] method.

#### 15.08.2012
- Version 0.4
- Added "isFlagShip" method.
- Added character encoding support. This comes with methods: setEncoding, encoding and getRecognizedEncodings.
- Fixed reading 3-length "V" columns (dates) so they are actually read as "D" column.
- Implemented [vacuum] method.
- Implemented [tell] method.
- Introduced new datetime conversion methods: unixTimeToJulianDate, shortDateToUnixTime, unixTimeToShortDate.
- Fixed reading column length from header (for big lengths).
- Fixed internal methods: convertShortDate and convertShortDateToBin to support dates before 1970.
- Fixed reading "@" columns.

#### 09.08.2012
- Version 0.3.
- Field type "T" is now read correctly.
- Added julianDateToUnixTime function to convert jualian dates after 1970 to unixtime, which is more Tcl-like.
- Fixed flushInitialHeader when creating new dbf file in any month except of 4th quater of the year - didn't work at all.

#### 01.08.2012
- Version 0.2.
- Fixed reading of Visual FoxPro files.
- "D" type is now read as string, not as unixtime, cause unixtime doesn't deal with dates before 1970.
- "C" type is now trimmed from left side, so there are no extra white spaces before the actual value.
- Added pkgIndex.tcl, making a complete Tcl package.

#### 01.11.2011
- First release: 0.1.
