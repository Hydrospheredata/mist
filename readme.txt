This directory contains ISH/ISD data in directories by year.  Please note that ISH and ISD refer to
the same data--Integrated Surface Data, sometimes called Integrated Surface Hourly.

Updated files will be listed in the updates.txt file, and will be stored both in the /updates directory
and in the respective data directories by year.  However, for the current year, updates will
be more frequent and will not be indicated in the updates.txt file.  Please note that all data files
are compressed as indicated by the "gz" extension and can be uncompressed using Gunzip, WinZip or other
similar software applications.

The filenames correspond with the station numbers listed in the ish-history.txt file described below -- 
eg, 723150-03812-2006 corresponds with USAF number 723150 and WBAN number 03812.

Extensive updates to the dataset took place in November 2004, as Version 2.3 of the dataset was 
released, and periodic updates have continued since then.

Other files included in the main directory are: 

- The station history, both in ASCII text format and in Excel format:
ish-history.txt and ish-history.csv -- note that this station history covers the full
period of record for ISD, so some stations listed in this file have no recent data.

- A country list with the FIPS ID for each country:
country-list.txt

- An inventory of the ISD data by station-year-month:
ish-inventory.txt and ".csv" (compressed - ".z" file)

- The data format documentation, required to read and interpret the data:
ish-format-document.pdf

- A technical report describing the ISD development process:
ish-tech-report.pdf

- A paper describing the ISD quality control process:
ish-qc.pdf

- A list of known systematic problems in the data:
isd-problems.pdf, isd-problems.doc

- A java program to convert the ISD data into a space-delimited / abbreviated format:
ishJava.

- Documentation for usage of ishJava:
ishJava_ReadMe.pdf

- Format documentation for the space-delimited format produced by ishJava:
ish-abbreviated.txt

- A listing of updates performed on the ISD data and the dates they occured:
updates.txt

- Other software to use with the data:
/software directory now contains:
1) A fortran program (several subroutines called by "main1.f") to summarize the data by day.
2) A perl program to display/read the data.
3) ftp://ftp.ncdc.noaa.gov/pub/data/ish - directory contains additional software for use with ISD.

IMPORTANT NOTE:  The non-U.S. data in ISD are subject to WMO Resolution 40 restrictions, and cannot be 
redistributed to other users or customers.

The directory and all contents are accessible via web browser:
http://www1.ncdc.noaa.gov/pub/data/noaa/
Or via direct ftp:
- Ftp to ftp.ncdc.noaa.gov
- login as ftp or as anonymous
- password is your email address
- cd to /pub/data/noaa
- get or mget files as needed--in ascii or bin mode (bin required for any non-ascii file)
- cd to individual directories by year to get data files

Any inquiries, please contact mark.lackey@noaa.gov for technical problems, or
ncdc.orders@noaa.gov for other issues.
 
Neal Lott
Data Access Branch
NOAA's National Climatic Data Center
