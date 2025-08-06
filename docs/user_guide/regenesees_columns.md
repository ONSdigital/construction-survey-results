# Regenesses New Columns Description

The following table adds further detail to the new columns created by Regeneeses run built into RSurveyMethods. Columns created in the construction python pipeline have not been included here, for this information see [constructoin output columns][cons-link]

| column name                | description         |
|----------------------------|---------------------|
| Total.winsorised_value     | The value for the winsorised adjusted response per question number per period, calculated using cell number as strata. |
| CV.Total.winsorised_value  | Coefficient of variation for the winsorised adjusted response per question number per period. |
| SE.Total.winsorised_value  | Standard error for the winsorised adjusted response per question number per period.  |
| frotover_converted_for_regen |Not strictly output, but we need to convert values for frotover to work correctly in Regenesses. The function cannot handle frozen turnovers of 0, therefore we replace these with $10^{-6}$|

The following columns were also present in the Regenesses extract but not in the smaller output dataframe, unsure where these are being created.

| column name                | description         |
|----------------------------|---------------------|
| X                          | Present in recent regen extract, but not in regen_results_v1 file?|
| X290_flag                  |Present in recent regen extract, but not in regen_results_v1 file?|

[cons-link]:
