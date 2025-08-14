# Regenesses New Columns Description

The following table adds further detail to the new columns created by Regeneeses run built into RSurveyMethods. Columns created in the construction python pipeline have not been included here, for this information see [constructoin output columns][cons-link]

| column name                | description         |
|----------------------------|---------------------|
| Total.winsorised_value     | The value for the winsorised adjusted response per question number per period, calculated using cell number as strata. |
| CV.Total.winsorised_value  | Coefficient of variation for the winsorised adjusted response per question number per period. |
| SE.Total.winsorised_value  | Standard error for the winsorised adjusted response per question number per period.  |
| frotover_converted_for_regen |Not strictly output, but we need to convert values for frotover to work correctly in Regenesses. The function cannot handle frozen turnovers of 0, therefore we replace these with $10^{-6}$|
|extcalweights | This is design weight multiplied by calibration factor ($a_\text{weight} \cdot g_\text{weight}$)|

The following columns were also present in the Regenesses extract but not in the smaller output dataframe, unsure where these are being created.

| column name not from ReGen            | description         |
|----------------------------|---------------------|
|population_count, population_turnover_sum, sample_count, sample_turnover_sum | Cannot find where this is produced within Rsurveymethods, we think this is created in Construction main pipeline but not sure where!|
| X / X290_flag | Cannot find where this is created in Regen, when looking at extracts, X and X290_flag are 4th and 6th columns, all other created columns are on the far right. This suggests to us these are not created in Regen but elsewhere or supplied in input data |


[cons-link]:
