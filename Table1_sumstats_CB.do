clear 
// path to dropbox
//global dropbox = "D:\Dropbox"
global dropbox = "C:\Users\begenau\Dropbox"


 /*
//drop if date<19960331
/*
// Load big bank definition //

import delimited using "`dir_rw_data'\bigIndicator.csv", clear
rename v1 rssdid 
rename v2 date
rename v3 bigIndicator


gen year  =  floor(date/10000)
gen month  = floor((date - year*10000)/100)
gen quarter = month ==3
replace quarter  = 2 if month ==6
replace quarter  = 3 if month ==9
replace quarter  = 4 if month ==12

gen yq = yq(year,quarter)
format yq %tq
 drop date year month quarter
 
tempfile bigIndicator
save `bigIndicator'
 */
 
 
use  "`dir_rw_data'\ratewatch_full_sav25k.dta"  , clear

// Number of follower branches in same state 
 
keep rate_setter acct_nbr_rt acct_nbr_loc yq  stcntybr state_fps
 
bysort acct_nbr_rt yq: gen rate_setting_state = state_fps if rate_setter
egen maxtemp = max(rate_setting_state), by(acct_nbr_rt yq)
replace rate_setting_state = maxtemp
drop maxtemp

gen same_state = rate_setting_state == state_fps if ~rate_setter
egen same_state_count = total(same_state) ,  by(yq acct_nbr_rt)
egen nbr_FLW_branches = nvals(acct_nbr_loc) if ~rate_setter , by(yq acct_nbr_rt)
egen max = max(nbr_FLW_branches ) ,  by(yq acct_nbr_rt)
replace nbr_FLW_branches = max 
drop max
 

drop if mi(rate_setting_state)
drop if mi(nbr_FLW_branches)

collapse (mean) nbr_FLW_branches same_state_count , by(yq acct_nbr_rt)

gen same_state_fraction = same_state_count/nbr_FLW_branches

mean(same_state_fraction) // 96.02 % of follower branches are in the same state
count 
count if same_state_fraction==1
//  410K rate setting branches, 366K had follower branches in the same state

 */
// Load data  //
use "bankleveldata_05112021.dta", clear 
 
// sample ratewatch merge 
drop if year <2001

gen month  = floor((date - year*10000)/100)
gen quarter = month ==3
replace quarter  = 2 if month ==6
replace quarter  = 3 if month ==9
replace quarter  = 4 if month ==12

gen yq = yq(year,quarter)
format yq %tq

replace cert = fdiccertificatenumber if mi(cert) & ~mi(fdiccertificatenumber)
 
rename _merge mergeC2Bdata
 
//egen aggAssets = total(asset) , by(yq)

//merge 1:1 yq rssid using `bigIndicator', keep(1 3) nogen

merge 1:m yq rssdid cert using "ratewatch_full_.dta"  , keep(match) nogen 
//drop i f date<19960331
 
preserve 

// Share of deposits in follower branches (on avearge)

br yq rssdid cert rate_setter acct_nbr_loc depsumbr

egen total_dep= total(depsumbr), by(yq)
egen total_flw_dep= total(depsumbr) if ~rate_setter, by(yq acct_nbr_rt)
egen max = max(total_flw_dep), by(yq acct_nbr_rt)
replace total_flw_dep = max if mi(total_flw_dep) 


collapse (mean) total_dep total_flw_dep, by(yq acct_nbr_rt)
collapse (mean) total_dep (sum) total_flw_dep, by(yq)

gen ratio = total_flw_dep /total_dep
mean(ratio)

 /*
restore  
*/
// Snapchat only
keep if yq == yq(2005,4)

egen has_network = max(network_branch) , by(yq cert)

tempfile inbw
save `inbw'

collapse (mean) asset has_network, by(yq rssdid)

egen asset_rank = rank(asset), by(yq)
egen xrank = xtile(asset_rank), by(yq) nq(10)

egen uses_network = total(has_network) , by(yq xrank)

merge 1:m rssdid yq using `inbw', nogen

//egen sizegroup   = xtile(asset ), by(yq) nq(11)

bysort rssdid yq: gen obs = _n

egen aux = total(asset) if obs==1, by(yq)
egen sizeau = total(asset) if obs==1, by(yq xrank)
gen maxa = sizeau /aux 
egen aggAssetShr = max(maxa) , by(yq rssdid )
drop aux sizeau maxa

egen aux = total(deposits) if obs==1, by(yq)
egen sizeau = total(deposits) if obs==1, by(yq xrank)
gen maxa = sizeau /aux 
egen aggDepShr =  max(maxa) , by(yq rssdid )
drop aux sizeau  maxa

egen aux = total(loan) if obs==1, by(yq)
egen sizeau = total(loan) if obs==1, by(yq xrank)
gen maxa = sizeau /aux 
egen aggLoanShr =  max(maxa) , by(yq rssdid )
drop aux sizeau  maxa

egen aux = total(loan_comm) if obs==1, by(yq)
egen sizeau = total(loan_comm) if obs==1, by(yq xrank)
gen maxa = sizeau /aux 
egen aggLoanCIShr = max(maxa) , by(yq rssdid )
drop aux sizeau  maxa
 
egen HHI_cat = xtile(HHI_branch) , by(year) nq(3)

gen HHI_high = HHI_cat==3
 
   
egen nbr_HHIFLW_high  = total(HHI_high) if ~rate_setter , by (yq xrank)
egen aux = max(nbr_HHIFLW_high ) , by(yq xrank)
replace nbr_HHIFLW_high  = aux if mi(nbr_HHIFLW_high )
drop aux

egen nbr_RTS_branches = nvals(acct_nbr_rt) if rate_setter, by(yq xrank)
egen aux = max(nbr_RTS_branches ) , by(yq xrank)
replace nbr_RTS_branches  = aux if mi(nbr_RTS_branches )
drop aux

// done in ratewatch_branch_data
// bysort acct_nbr_rt yq: gen network_branch = 1 & ~rate_setter
// egen nbr_flw_byRTS = nvals(acct_nbr_loc)  if ~rate_setter, by(yq acct_nbr_rt)
// egen aux = max(nbr_flw_byRTS ) , by(yq acct_nbr_rt)
// replace nbr_flw_byRTS  = aux if mi(nbr_flw_byRTS )
// drop aux
// mvencode nbr_flw_byRTS , mv(0)
// gen independent_branch = rate_setter & nbr_flw_byRTS==0
//
// replace network_branch = rate_setter & independent_branch==0 if network_branch== 0 

egen count_network = total(rate_setter)  if network_branch, by(yq cert)
egen aux = max(count_network ) , by(yq cert)
replace count_network  = aux if mi(count_network )
drop aux 

mvencode count_network , mv(0)  override

egen nbr_network_branches= total(network_branch), by(yq xrank)
egen nbr_independent_branches= total(independent_branch), by(yq xrank)
 
 
// Number of total branches 
egen nbr_Tot_branches = nvals(acct_nbr_loc), by(yq xrank)
gen  nbr_FLW_branches = nbr_Tot_branches - nbr_RTS_branches
egen nbr_banks = nvals(cert), by(yq xrank)

gen ratio = nbr_network_branches/ nbr_Tot_branches
gen highratio = nbr_HHIFLW_high/ nbr_Tot_branches


egen min = min(HHI_branch) , by(yq xrank state_fps)
egen max = max(HHI_branch) , by(yq xrank state_fps)
gen HHIrange = max - min
drop min max
egen min = min(apy_raw) , by(yq xrank state_fps)
egen max = max(apy_raw) , by(yq xrank state_fps)
gen RateRange = max - min
drop min max
 

egen min = min(HHI_branch), by(acct_nbr_rt yq)
egen max = max(HHI_branch), by(acct_nbr_rt yq)
gen HHIrangeByRSB =  max - min
drop min max
egen min = min(apy_raw), by(acct_nbr_rt yq)
egen max = max(apy_raw), by(acct_nbr_rt yq)
gen RaterangeByRSB =  max - min
drop min max
gen depsumbrM = depsumbr/1000


egen meanD_hHHI = mean(depsumbrM) if HHI_cat==3, by(cert yq)
egen maxD  = max(meanD_hHHI), by(cert yq)
replace meanD_hHHI = maxD if meanD_hHHI ==.
drop maxD 
 
 
egen meanD_lHHI = mean(depsumbrM) if HHI_cat==1, by(cert yq)
egen maxD  = max(meanD_lHHI), by(cert yq)
replace meanD_lHHI = maxD if meanD_lHHI ==.
drop maxD  

egen sumD_hHHI = total(depsumbrM) if HHI_cat ==3, by(yq xrank)
egen maxD  = max(sumD_hHHI), by(yq xrank)
replace sumD_hHHI = maxD if sumD_hHHI ==.
drop maxD  
replace sumD_hHHI  = sumD_hHHI/1000

egen sumD_lHHI = total(depsumbrM) if HHI_cat ==1, by(yq xrank)
egen maxD  = max(sumD_lHHI), by(yq xrank)
replace sumD_lHHI = maxD if sumD_lHHI ==.
drop maxD  
replace sumD_lHHI  = sumD_lHHI/1000

egen sumD_hHHI_FlwBr = total(depsumbrM) if HHI_cat ==3 & ~rate_setter, by(yq xrank)
egen maxD  = max(sumD_hHHI_FlwBr), by(yq xrank)
replace sumD_hHHI_FlwBr = maxD if sumD_hHHI_FlwBr ==.
drop maxD  
replace sumD_hHHI_FlwBr  = sumD_hHHI_FlwBr/1000


collapse (mean) nbr*  uses_network agg* depsumbrM ratio HHIrange  count_network RateRange HHIrangeByRSB RaterangeByRSB independent_branch network_branch meanD* sum* high*  , by(yq xrank)


lab var nbr_Tot_branches "Number of Branches"
lab var nbr_RTS_branches "Number of Rate Setting Branches"
lab var nbr_HHIFLW_high "Number of high HHI Follower Branches"
lab var nbr_banks "Number of Banks"
lab var uses_network "Number of Banks using Networks"
lab var nbr_independent_branches "Number of Independent Branches"
lab var nbr_network_branches "Number of Network Branches"
lab var count_network "Avg. Number of Networks by bank"
lab var aggAssetShr "Agg. Asset Share"
lab var aggLoanShr "Agg. Loan Share"
lab var aggDepShr "Agg. Deposit Share"
lab var aggLoanCIShr "Agg. Business Loan Share"
lab var depsumbrM "Deposits per Branch (\_$ M)"
lab var ratio "Network Branches to Total Ratio"
lab var highratio "Followers in high HHI to Total Branch Ratio"
lab var HHIrangeByRSB "Geographic HHI range of Follower Br."
lab var RaterangeByRSB "Geographic Rate range of Follower Br."
lab var meanD_hHHI "Deposits per Branch in High HHI Counties (\_$ M)"
lab var meanD_lHHI "Deposits per Branch in Low HHI Counties (\_$ M)"
lab var sumD_hHHI "Sum of Deposits in High HHI Counties (\_$ B)"
lab var sumD_lHHI "Sum of Deposits in Low HHI Counties (\_$ B)"
lab var sumD_hHHI_FlwBr "Sum of Follower Branch Deposits in High HHI Cts  (\_$ B)"

format nbr* %12.0fc
 estpost tabstat nbr_banks uses_network nbr_Tot_branches nbr_RTS_branches nbr_network_branches nbr_independent_branches    ratio nbr_HHIFLW_high   highratio  aggAssetShr aggDepShr aggLoanShr aggLoanCIShr depsumbrM   mean*  sumD_hHHI sumD_hHHI_FlwBr      HHIrangeByRSB  RaterangeByRSB , by(xrank) statistics(mean) ///
columns(statistics) listwise   nototal 
 

esttab ., cells("mean(fmt(%15.0f))") label  noobs  
esttab using   "SumStats.tex", replace booktabs    cells((mean(fmt(0 0 0 0 0 0  2  0 2  2 2 2  2 1 1 1 0 0 2  2 ))) ) mlabel(none) ///
collabels("")  unstack noobs nonote  nonumber wide label substitute(\_$ \\$ )  mgroups("Bank Deciles", pattern( 1 0 0 0 0 0)    prefix(\multicolumn{@span}{c}{) suffix(}) span erepeat(\cmidrule(lr){@span})) 

 

  


    


/*
keep if year >=2001

gen month  = floor((date - year*10000)/100)
gen quarter = month ==3
replace quarter  = 2 if month ==6
replace quarter  = 3 if month ==9
replace quarter  = 4 if month ==12

gen yq = yq(year,quarter)
format yq %tq

replace cert = fdiccertificatenumber if mi(cert) & ~mi(fdiccertificatenumber)
 
rename _merge mergeC2Bdata
 
egen aggAssets = total(asset) , by(yq)

merge 1:m yq rssdid cert using "`dir_rw_data'\ratewatch_full_sav25k.dta"  , keep(match) nogen 
//drop if date<19960331

  
egen sizegroup   = xtile(asset ), by(yq) nq(10)
egen num_branch_bySIZE = nvals(acct_nbr_loc) , by(yq sizegroup)
egen num_RTS_branch_tot = nvals(acct_nbr_rt) , by(yq sizegroup)

egen asset_sizegroup = total(asset) , by(yq sizegroup)
//egen num_RTS_branch_tot = total(num_RTS_branch), by(yq sizegroup)
egen num_banks = nvals(cert) , by(sizegroup yq)

egen iqrHHI = iqr(HHI_branch), by(cert )
egen iqrRate = iqr(apy) , by(cert yq)

gen assetB = asset /1000000
gen loanB  = loan /1000000
gen depositsB  = deposits/1000000
gen loanA  = loan/asset
gen depA  = deposits/asset
gen timeDepositShr =  deposits_dom_nontrans_time/deposits 
gen aggAssetShr =  asset/aggAssets
gen depositsAvgXbrM = depositsAvgXbr/1000 
 
lab var assetB "Assets (Bill)"
lab var loanB "Loans (Mill)"
lab var depositsB "Deposits (Bill)"
lab var loanA "Loan / Asset"
lab var depA "Deposits / Asset"
lab var timeDepositShr "Time Deposits / Deposits"
lab var aggAssetShr "Asset / Agg. Assets"
lab var num_branch_bySIZE "Nbr Branches"
lab var num_branch "Nbr Branches p. Bank"
lab var depositsAvgXbrM "Deposits (M) per Branch"
lab var num_RTS_branch_tot "Nbr RTS Branches"
lab var num_RTS_branch "Nbr RTS Branches p. Bank"
lab var depositRTSshr "RTS Deposit Share"
lab var HHI_branch "Bank HHI (eq-weighted)"
lab var iqrHHI "Bank HHI: IQ-Range"
lab var num_banks "Nbr Banks"
 
 estpost tabstat num_banks assetB loanB depositsB loanA depA timeDepositShr aggAssetShr num_branch_bySIZE ///
 num_branch  depositsAvgXbrM  num_RTS_branch_tot num_RTS_branch depositRTSshr ///
 HHI_branch iqrHHI iqrRate, by(sizegroup) statistics(mean) ///
columns(statistics) listwise 
esttab  using   "`dir_table'\Table1_sumstats_CB.tex", replace booktabs cells(mean(fmt(2))) nostar unstack noobs nonote nomtitle nonumber wide label  

/*
eststo clear
by sizegroup: eststo: quietly estpost summarize ///
 assetB loanB depositsB loanA depA timeDepositShr aggAssetShr num_branch_bySIZE ///
 num_branch  depositsAvgXbrM  num_RTS_branch_tot num_RTS_branch depositRTSshr ///
 avgHHI iqrHHI
esttab, cells("mean") label nodepvar 


taboutusing "`dir_table'\.tex", replace  ///
c(mean wage se) f(2 2) clab(Mean_wage SE) ///
sum svy npos(lab) ///
rep ///
clab(10%tile 20%tile 30%tile 40%tile 50%tile 60%tile 70%tile 80%tile 90%tile 100%tile ) ptotal(none)  ///
sum style(tex) bt topf(top.tex) botf(bot.tex) topstr(9)   botstr(RateWatch Data)	 
	

tabout stalpbr if state_fps>29 &  yq ==yq(2006,1)   c(mean num_banks_perstate mean num_followerbranches_perstate mean num_ratebranches_perstate mean apy mean avgHHI min apy max apy  iqr avgHHI) ///
f(0c 0c 0c 3 3 3 3 3) ///
clab(Banks Branches Rate_Setters Rate HHI Rate Rate HHI) cl2(2-6 7-8 9-9) ptotal(none) npos(tufte) ///
sum style(tex) bt topf(top.tex) botf(bot.tex) topstr(9)   botstr(RateWatch Data)	 
	
 
 
