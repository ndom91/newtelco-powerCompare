# Newtelco Gmbh - Power Usage
Author: ndomino@newtelco.de  
Release: 06.02.2019
 
## Intro

This is a simple python script which runs the first of each month and compares the power usage of customers to monthly allowances.

- The power allowances are stored in an instance of [netbox](https://github.com/netbox-community/netbox) in a postgresql db.   
- The current power usage is queried daily via NZR's VADEV application, transformed, and stored in a mysql db in our custom crm application. 

Both the allowance and monthly usages are queried in this python script, transformed and compared via numpy and pandas.

Finally, it generates an email highlighting all power counters / customers whose usage have exceed their allowance, for example: 

```
Contract: 123456

Counter        name       Rack   Month   Usage   DC    Contract   AC
337  32123212  R1234 R&R  R1234  201907  1126.0  NaN   141615     3000.0
336  32123213  R1234 R&R  R1234  201907  1177.0  NaN   141615     3000.0

Monthly Usage: 3.1 kW
Allowed Usage: 3.0 kW
Over Usage (Überverbrauch): 0.1 kW
```

This simplified overview is augmented by an excel sheet attached to the notification emails with containing all the raw data. 

## Getting Started

1. Clone this repo - `cd https://github.com/ndom91/powerCompare /opt/powercompare`
2. Symlink the bash script into a directory in your $PATH - `sudo ln -s /opt/powercompare/powercompare.sh /usr/local/bin/powercompare`
3. Now you can call it from anywhere and/or setup a cronjob to run monthly, for example. 

```
 ndo@newtelco-ftp> powercompare
 Newtelco Powercompare
 Commandline Arguments:
 --date | -d        YEAR + MONTH i.e. 201904
 --sendmail | -m    Boolean, send mail or not
 --help | -h        Print this usage information

 ndo@newtelco-ftp> powercompare -d 201904 -m 
```

### Output

Default output includes 
1. Mails to: billing[at]newtelco.de, order[at]newtelco.de, sales[at]newtelco.de, ndomino[at]newtelco.de  
2. Excel Attachment
