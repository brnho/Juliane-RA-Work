local fromscratch = 1
local do_rate_files = 1
local do_acc_files = 1
local product    sav25k



if `fromscratch' ==1 {
	if `do_acc_files'==1 {
/*--------------------------------------------------------------------------*/
*1: Deposit Certificate History
/*--------------------------------------------------------------------------*/
import delimited "/zfs/data/ratewatch/RateWatchScholar_7_29_22/RW_DepositDataFeedMASTER_thruMay2020/RW_DepositDataFeedMASTER/DepositCertChgHist.txt", clear

gen acct_nbr_loc = acctnbr 

// generate date  
drop chgtms
egen date = sieve(chgdt), omit(/)
gen year =substr(date, 5,8) 
gen month =substr(date, 1,2)
destring   date month year, replace
 
gen date_m  = ym(year,month)
format date_m %tm
drop date year month

gen branchopen = description =="Branch Open"
gen branchopen_date = date_m if branchopen==1
egen aux = max(branchopen_date ) , by(acct_nbr_loc)
replace branchopen_date  = aux
drop aux

gen branchclose = description =="Branch Close"
gen branchclose_date= date_m if branchclose ==1
egen aux = max(branchclose_date ) , by(acct_nbr_loc)
replace branchclose_date  = aux
drop aux
	
 
// https://www.ncua.gov/--> National Credit Union Association
bysort acct_nbr_loc: gen aux = max(fromncuanbr, toncuanbr)
gen iscreditunion = aux>0 & ~mi(aux)
drop aux acctnbr
format branchopen_date branchclose_date   %tm

/*gen switch_F2F = (fromfdicnbr>0 & tofdicnbr>0)
gen switch_F2C = (fromfdicnbr>0 & toncuanbr>0)
gen switch_C2F = (fromncuanbr>0 & tofdicnbr>0)
gen switch_C2C = (fromncuanbr>0 & toncuanbr>0)
*/
rename date_m date_m_history

duplicates tag   acct_nbr_loc date_m_history, gen(tag)
egen fromfdicnbr_max = max(fromfdicnbr)  if tag ==1, by(acct_nbr_loc date_m_history  )
egen   tofdicnbr_max = max(tofdicnbr)    if tag ==1, by(acct_nbr_loc date_m_history  )
egen    tocuanbr_max = max(toncuanbr)    if tag ==1, by(acct_nbr_loc date_m_history  )
egen  fromcuanbr_max = max(fromncuanbr)  if tag ==1, by(acct_nbr_loc date_m_history  )


replace fromfdicnbr = fromfdicnbr_max if  tag ==1
replace tofdicnbr   = tofdicnbr_max   if  tag ==1
replace tocuanbr    = tocuanbr_max    if  tag ==1
replace fromcuanbr  = fromcuanbr_max  if  tag ==1

drop *_max
drop if tag==1 & mi(description) // consolidate branch open / closure and cert change on one date

//br tag acct_nbr_loc date_m_history description from* to* chgdt if tag>1
// several cert changes within a month
sort acct_nbr_loc chgdt 
bysort acct_nbr_loc date_m_history (chgdt): gen obsnum = _n 
 
sort acct_nbr_loc chgdt 
bysort acct_nbr_loc date_m_history (chgdt): replace description = description[_n-2] if tag ==2 & obsnum==3 & mi(description)
bysort acct_nbr_loc date_m_history (chgdt): replace description = description[_n-1] if tag ==2 & obsnum==3 & mi(description)
// keep only the last obs. 
drop if tag==2 & obsnum<3 & ~mi(obsnum)
drop tag obsnum
//  duplicates report acct_nbr_loc date_m_history
// no duplictates!

 
// duplicate to prep merge
expand 2 , gen(dupindicator)
gen  prd_typ_join ="MM" 
replace prd_typ_join ="CD" if  dupindicator
drop dupindicator
tempfile history
save `history'
/*--------------------------------------------------------------------------*/
*2: Rate Accounts
/*--------------------------------------------------------------------------*/
  
import delimited "/zfs/data/ratewatch/RateWatchScholar_7_29_22/RW_DepositDataFeedMASTER_thruMay2020/RW_DepositDataFeedMASTER/Deposit_acct_join.txt", clear

rename v1 acct_nbr_loc
rename v2 acct_nbr_rt
rename v3 prd_typ_join
rename v4 eff_date

// focus on those products only
keep if prd_typ_join =="MM" | prd_typ_join =="CD"
 // generate date 
egen date = sieve(eff_date), omit(-)
gen date_des =substr(date, 1,6) 
destring date_des  date, replace
gen year  = floor(date_des/100)
gen month = floor(date_des) - year*100
drop date_des
gen date_m  = ym(year,month)

gen qrt = 1
replace qrt =2 if month == 4 | month ==5 | month ==6
replace qrt =3 if month == 7 | month ==8 | month ==9
replace qrt =4 if month >=10 

gen date_q  = yq(year,qrt)
format date_q %tq

gen  date_m_history = date_m

 egen id_loc = group(  acct_nbr_loc  prd_typ_join)

sort id_loc date_m_history

duplicates tag id_loc date_m, gen(tag)

sort prd_typ_join id_loc date
bysort id_loc (date_m): gen obs_val = _n
egen last_dub = max(obs_val) if tag>0, by(id_loc date_m)
 
drop if tag>0 & last_dub>obs_val
drop tag last_dub obs_val 

merge 1:m acct_nbr_loc prd_typ_join date_m_history using `history'
rename _merge _merge_whist
///append using `history'
 
replace date_m = date_m_history if mi(date_m)
drop date_m_history
br _merge_* acc* prd_typ_join date_m
format date_m %tm branchopen


sort prd_typ_join acct_nbr_loc  date_m
mvencode branchopen branchclose ,mv(0) override

egen max_aux = max(branchopen_date), by(acct_nbr_loc)
replace branchopen_date = max_aux
drop max_aux
egen max_aux = max(branchclose_date), by(acct_nbr_loc)
replace branchclose_date= max_aux
drop max_aux

rename prd_typ_join producttype

 sort producttype acct_nbr_loc  date_m

 drop id*
 
 egen id_loc = group(  acct_nbr_loc  producttype)

 tempfile auxfile
save `auxfile', replace


egen branchopen_date_aux       = max(branchopen_date), by(id_loc)

// Set first observation to branch open time or 2001q1 whichever is later

sort id_loc date_m 

collapse (first) acct*  date_m  date_q producttype     branchopen_date_aux , by(id_loc)
// add first obs of branchclose or 2001q1 to all accounts

    gen min_date = branchopen_date_aux  
replace min_date = ym(2001,1)  if min_date <ym(2001,1)  | mi(min_date)
drop branchopen_date_aux 
replace date_m = min_date if date_m< min_date
replace date_q = qofd(dofm(date_m))
format %tq date_q

merge 1:1 id_loc date_m acct_nbr_loc producttype using 	`auxfile' , nogen 
// merge back with original data

egen branchopen_date_aux      = max(branchopen_date), by(id_loc)
replace branchopen_date = branchopen_date_aux if mi(branchclose_date)
drop *_aux
sort id_loc date_m

// drop all observation before 2001
drop if date_m < ym(2001,1)
 
tempfile auxfile
save `auxfile', replace // that data

egen branchclose_date_aux      = max(branchclose_date), by(id_loc)

sort id_loc date_m

collapse (last) acct* date_m date_q  producttype branchopen_date branchclose_date_aux , by(id_loc)

// add last obs 2020q4 or branchclose which ever is earlier to all accounts
gen max_date  = branchclose_date_aux 
replace max_date = ym(2020,12) if branchclose_date_aux>ym(2020,12)  | mi(branchclose_date_aux)
replace date_m = max_date  if date_m< max_date 
replace date_q = qofd(dofm(date_m))
format %tq date_q
drop branchclose_date_aux  
merge 1:1 id_loc acct_nbr_loc producttype date_m using 	`auxfile' // merge back with original data
 sort producttype id_loc date_m
//append using 	`auxfile', force // merge back with original data


 //br acct* id_loc date_m producttype branch*  eff* chg* from* to*  if acct_nbr_loc=="WY00600045"
 //sdfakjalsdkf
br acct* id_loc date_m producttype branch* eff* chg* from* to* //if acct_nbr_loc=="WY00600063"
// note: not dupblicates at this time

replace date_q = qofd(dofm(date_m)) if mi(date_q)


gen aux = mi(acct_nbr_rt)
egen count_miss = total(aux), by(id_loc)
drop aux

bysort id_loc: gen obsval = _N
gen acct_rt_miss = count_miss ==obsval 
 
// deal with obs that have no account rate setting info
replace acct_nbr_rt = acct_nbr_loc if acct_rt_miss
drop acct_rt_miss obsval count_miss

gen          acct_rt_miss = mi(acct_nbr_rt)
egen count_miss = total(acct_rt_miss), by(id_loc)
bysort id_loc (date_m): gen obsval = _n
 
 
 	// takes care of single missings 
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n-1] if  acct_rt_miss 	& ~mi(acct_nbr_rt[_n-1]) & obsval>1 
	
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss 	& ~mi(acct_nbr_rt[_n+1]) & obsval==1 
 
 // takes care of first two missings  
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2]) & obsval==1  & count_miss ==2

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1])  & obsval==2 & count_miss ==2

// takes care of first three missings  
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+3] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+3]) & obsval==1  & count_miss ==3

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2])  & obsval==2 & count_miss ==3

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1])  & obsval==3 & count_miss ==3

// takes care of first four missings 
 
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+4] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+4]) & obsval==1   & count_miss ==4

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+3] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+3])  & obsval==2 &  count_miss ==4

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2])  & obsval==3 & count_miss ==4

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1])  & obsval==4 & count_miss ==4

drop count_miss acct_rt_miss
gen          acct_rt_miss = mi(acct_nbr_rt)
egen count_miss = total(acct_rt_miss), by(id_loc)

// with remaining missings (still 60 k) 

 // takes care of first two missings  
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2]) & obsval==1  & count_miss ==2

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1])  & obsval==2 & count_miss ==2

// takes care of first three missings  
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+3] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+3]) & obsval==1  & count_miss ==3

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2])  & obsval==2 & count_miss ==3

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1])  & obsval==3 & count_miss ==3

// takes care of first four missings 
 
bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+4] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+4]) & obsval==1   & count_miss ==4

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+3] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+3])  & obsval==2 &  count_miss ==4

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2])  & obsval==3 & count_miss ==4

bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1])  & obsval==4 & count_miss ==4


// takes care of first five missings 
 
 	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+5] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+5]) & count_miss ==5 & obsval ==1

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+4] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+4]) & count_miss ==5 & obsval ==2
	
	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+3] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+3]) & count_miss ==5 & obsval ==3

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2]) & count_miss ==5 & obsval ==4

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1]) & count_miss ==5 & obsval ==5
 

// takes care of first six missings 
 
  	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+6] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+6]) & count_miss ==6 & obsval ==1
	
	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+5] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+5]) & count_miss ==6 & obsval ==2

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+4] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+4]) & count_miss ==6 & obsval ==3
	
	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+3] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+3]) & count_miss ==6 & obsval ==4

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2]) & count_miss ==6 & obsval ==5

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1]) & count_miss ==6 & obsval ==6	
	
	
// takes care of first seven missings  
 
 	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+7] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+7]) & count_miss ==7 & obsval ==1

  	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+6] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+6]) & count_miss ==7 & obsval ==2
	
	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+5] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+5]) & count_miss ==7 & obsval ==3

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+4] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+4]) & count_miss ==7 & obsval ==4
	
	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+3] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+3]) & count_miss ==7 & obsval ==5

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+2] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+2]) & count_miss ==7 & obsval ==6

	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+1] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+1]) & count_miss ==7 & obsval ==7	
	
/*
levelsof count_miss
foreach l of local levels {
	local i = `1'
	disp " run is now `l'"
	while i <= `l' {
	disp `i'	
	bysort id_loc (date_m): replace acct_nbr_rt = acct_nbr_rt[_n+`l'+1-`i'] if  acct_rt_miss  	& ~mi(acct_nbr_rt[_n+`l'+1-`i']) & count_miss ==`l' & obsval ==`i'
	local i = `i' + 1
	}
	
 }
	 */
drop count_miss acct_rt_miss 
/*gen          acct_rt_miss = mi(acct_nbr_rt)
egen count_miss = total(acct_rt_miss), by(id_loc)
sum count_miss,d  */
	 
// pick the correct first observation 

duplicates tag id_loc date_q, gen(test)

br acct* id_loc date_m producttype  eff* chg*  test from* to* if test>1
sort id_loc date_m
 
egen fromfdicnbrmax = max(fromfdicnbr) if test>0, by(id_loc date_q)
egen tofdicnbrmax   = max(tofdicnbr) if test>0, by(id_loc date_q)
egen fromncuanbrmax = max(fromncuanbr) if test>0, by(id_loc date_q)
egen toncuanbrmax = max(toncuanbr) if test>0, by(id_loc date_q)

replace fromfdicnbr = fromfdicnbrmax if test>=1  & mi(fromfdicnbr)  
replace tofdicnbr = tofdicnbrmax if test>=1 & mi(tofdicnbr)   
replace fromncuanbr = fromncuanbrmax if test>=1 & mi(fromncuanbr)  
replace toncuanbr = toncuanbrmax if test>=1 & mi(toncuanbr)  

egen last_dub = max(obsval) if test>0, by(id_loc date_q)
 
drop if test>0 & last_dub>obsval
drop test last_dub obsval year month  date_m    qrt *max date max_date min_date description _merge* 
  
tsset id_loc date_q

tsfill

bysort id_loc: carryforward acct_nbr_loc, gen(acct_nbr_loc_n)
bysort id_loc: carryforward acct_nbr_rt, gen(acct_nbr_rt_n)
bysort id_loc: carryforward producttype, gen(producttype_n)
bysort id_loc: carryforward fromfdicnbr, gen(fromfdicnbr_n)
bysort id_loc: carryforward tofdicnbr, gen(tofdicnbr_n)
bysort id_loc: carryforward fromncuanbr, gen(fromncuanbr_n)
bysort id_loc: carryforward toncuanbr, gen(toncuanbr_n)
 
drop acct_nbr_loc acct_nbr_rt producttype fromfdicnbr  tofdicnbr fromncuanbr toncuanbr 

rename acct_nbr_loc_n acct_nbr_loc
rename acct_nbr_rt_n acct_nbr_rt
rename producttype_n producttype
rename fromfdicnbr_n fromfdicnbr
rename tofdicnbr_n tofdicnbr
rename fromncuanbr_n fromncuanbr
rename toncuanbr_n toncuanbr

gen acct_nbr = acct_nbr_loc

// merge with branch bank information from rate watch
merge m:1 acct_nbr  using  "Deposit_InstitutionDetails.dta" ,  keepusing(cert_nbr  uninumbr branches  rssd_id cnty_fps state_fps  msa  zip)  keep(3) nogen 

rename rssd_id rssdid_RW
drop if mi(id_loc)

xtset  id_loc date_q
bysort id_loc (date_q): replace fromfdicnbr = F.fromfdicnbr if mi(fromfdicnbr ) 
bysort id_loc (date_q): replace tofdicnbr = F.tofdicnbr if mi(tofdicnbr ) 
bysort id_loc (date_q): replace fromncuanbr = F.fromncuanbr if mi(fromncuanbr ) 
bysort id_loc (date_q): replace toncuanbr = F.toncuanbr if mi(toncuanbr ) 
// get historically accurrate certificate number 
replace cert_nbr = tofdicnbr if cert_nbr!=tofdicnbr & ~mi(tofdicnbr) & (tofdicnbr!=0)
// drop superfluous info 
drop id_loc branchopen branchclose is* from* to* 
 
// IMPORTANT DrOP if no CERT NBR
drop if mi(cert_nbr)
// not that cert_nbr in ratewatch does not uniquely identify fdic certificates, as cert of bank (fdic cert) might be identical to cert of a credut union which can't b found in https://banks.data.fdic.gov/bankfind-suite/bankfind
 
//  not all branches match but the vast mamority does

save "`dir_ratewatch'\ratesetloc.dta", replace
}
/*--------------------------------------------------------------------------*/
*3: Rate DAtA
/*--------------------------------------------------------------------------*/
	if `do_rate_files'==1 {
 // rate data
// 2014 2015 2016 2017 2018 2019 2020
foreach data in 2001 2002 2003   2004 2005 2006 2007 2008 2009 2010  2011 2012 2013 ///
                2014 2015 2016   2017 2018 2019 2020 {

import delimited "/zfs/data/ratewatch/RateWatchScholar_7_29_22/RW_DepositDataFeedMASTER_thruMay2020/RW_DepositDataFeedMASTER/depositRateData_clean_`data'.txt", clear bindquote(nobind)

disp "`data'"
gen broadtype  = substr(productdescription, 1,4)

gen cd12_yes  = broadtype =="12MC" 
replace cd12_yes  = 1 if broadtype =="12Mo"  & producttype =="CD"

gen sav_yes = (broadtype =="MM25" ) | (broadtype =="MM10" ) | (broadtype =="MM2." ) 
 
 keep if sav_yes==1 | cd12_yes  ==1

gen acct_nbr = accountnumber
egen date = sieve(datesurveyed), omit(-)
gen date_des =substr(date, 1,6)
destring date_des, replace
gen year  = floor(date_des/100)
gen month = floor(date_des) - year*100
drop date_des

gen date_m  = ym(year,month)
format date_m %tm

gen qrt = 1
replace qrt =2 if month == 4 | month ==5 | month ==6
replace qrt =3 if month == 7 | month ==8 | month ==9
replace qrt =4 if month >=10 

gen date_q  = yq(year,qrt)
format date_q %tq

keep acct_nbr productdescription  producttype   month year qrt date_q date_m  ///
    rate apy   mintoearn maxtoearn  cd12_yes sav_yes  
  
tempfile temp_`data'
save "temp_`data'", replace
  
}

 
  // Aggregate
use "temp_2001", clear


forvalues i = 2002/2020 {  
	
				  
append using 	 "temp_`i'", force
	 
}

// Select DSS products
   gen cd10k = strpos(productdescription , "CD10K")
    replace cd10k = cd10k>0
	
   gen sav25k = strpos(productdescription , "MM25K")
    replace sav25k = sav25k >0

   gen sav2d5k = strpos(productdescription , "MM2.5K")
   replace sav2d5k = sav2d5k >0
	
   gen sav10k = strpos(productdescription , "MM10K")
   replace sav10k = sav10k >0
	
	drop sav_yes
	generate sav_yes = (sav25k==1) | (sav2d5k==1) | (sav10k==1)
	
keep if cd10k ==1 | sav_yes ==1	 // focus on these two products only

sort acct_nbr productdescription date_m
// make quarterly sample
collapse (last)   rate  apy mintoearn maxtoearn  cd10k  sav*  ///
 year qrt month date_m producttype, by(acct_nbr  productdescription date_q)
 // merge with rate setting info
 
gen acct_nbr_rt = acct_nbr

replace producttype = "MM" if sav_yes ==1 & mi(producttype)
replace producttype = "CD" if sav_yes ==0 & mi(producttype)

save  "`dir_ratewatch'\ratewatch_panel.dta", replace
	}
 }  // from scratch done
 /*--------------------------------------------------------------------------*/
*4: select product: defined in local above  
/*--------------------------------------------------------------------------*/
   
  use "`dir_ratewatch'\ratewatch_panel.dta", clear
   
 keep if `product'==1 
 tempfile productrate
 save `productrate', replace
 
//------------------------------------------------
// 5. Combine data sets
//------------------------------------------------
// start with account location and rate location data
use "`dir_ratewatch'\ratesetloc.dta", clear
  
 // merge with rate data 
merge m:1 acct_nbr_rt date_q  producttype  using `productrate'  , keep( 3) nogen

gen apy_raw = apy
winsor2 apy, replace cuts(0.5 99.5) by(date_q) // winsorize data 


// merge with FFR data 
gen yq = date_q

// gen zip code 
gen zip_des =substr(zip, 1,5) 
destring zip_des, replace
drop zip
rename zip_des zipbr
// rename variables as in branch data
rename cert_nbr cert
//rename rssd_id rssdid
  

replace uninumbr = 0 if mi(uninumbr)

// merge with FDIC branch level data 
  merge m:1 cert   uninumbr  yq  using  "branchleveldata_forRWmerge.dta" ,  keepusing(HHI_branch  asset depsum depdom depsumbr rssdhcr pubtraded HHI_branch_9414 HHI_branch_9413 HHI_bank HHI_bank_avg bigbank rssdid stcntybr)   // do not force to be a branch in thefdic data 

  // gen county & state identifier 
gen stcntybr_rw = state_fps*1000 + cnty_fps 
replace stcntybr = stcntybr_rw if mi(stcntybr) & ~mi(stcntybr_rw )

drop if stcntybr ==.
 
gen SODmatch = _merge==3
    // fix HHI from unmatched branches
egen aux = max(HHI_branch), by(year stcntybr)
replace HHI_branch = aux if mi(HHI_branch) & ~mi(aux) 
drop aux

egen aux = mean(HHI_branch), by(year )
replace HHI_branch = aux if mi(HHI_branch) & ~mi(aux) 
drop aux
 
drop if mi(HHI_branch)
drop if _merge ==2
drop _merge 

// branch Characteristics

gen rate_setter = acct_nbr_loc == acct_nbr_rt
sort acct_nbr_rt yq acct_nbr_loc 
br acct* yq rate_setter 

egen nbr_flw_byRTS = nvals(acct_nbr_loc)  if ~rate_setter, by(yq acct_nbr_rt)
egen aux = max(nbr_flw_byRTS ) , by(yq acct_nbr_rt)
replace nbr_flw_byRTS  = aux if mi(nbr_flw_byRTS )
drop aux
mvencode nbr_flw_byRTS , mv(0)
gen independent_branch = rate_setter & nbr_flw_byRTS==0

gen follower_branch =  ~rate_setter & ~independent_branch
gen network_branch =  ~independent_branch

  // state street does not show  up in the rate data but in the info files of RW
  
 // merge with DSS Herfindahl index 
  gen fips = stcntybr 
merge m:1  fips using  "avgherfdepcty_DSS.dta"  , keep(match) nogen 

//------------------------------------------------
// 6. Save data - ready for analysis
//------------------------------------------------
drop eff_date cd10k sav* 
 
// save full sample
save "ratewatch_full_`product'.dta", replace
 
  	
// save DSS sample
keep if year<=2013 
keep if acct_nbr_loc ==acct_nbr_rt
save "ratewatch_DSS_`product'.dta", replace
 
 	
