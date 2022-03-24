# Medicines Text Mining Tool
Medicines text mining tool

***Repository owner: [NHS Digital Analytical Services](https://github.com/NHSDigital/data-analytics-services)***

***Email: datascience@nhs.net***

***To contact us raise an issue on Github or via email and we will respond promptly.***

# Clinical statement
NHS Digital's Interoperable Medicines Programme has been involved in establishing a flow of medicine data from secondary care to improve medication safety, gain insights into overprescribing, understand the overuse of antibiotics, and improve the treatments related to COVID-19. Initial investigatory work with trusts and suppliers concluded that the standard for describing and coding medicines (dictionary of medicines and devices - dm+d) is only partially adopted by secondary care organisations. To enable any medicines data collection from individual hospitals to be comparable across England the programme has developed text mining functionality to map a hospital medicine description to the closest match in the dm+d standard (this functionality is known as Medicines Text Mining Tool - MTMT). Where there are too many variations between the medicine description and the closest dm+d description the match will appear as unmapped (exceeds a threshold). The mapped outputs can only be used for the secondary uses of data and must not be used for direct care (e.g. must not be used to map a hospital drug dictionary for direct care use).
-	The Medicines Text Mining Tool has been developed from data derived from CareFlow Medicines Management electronic Prescribing and Medicines Administration (ePMA) systems utilised in 25 trusts in England. The data set contained 50,841,362 prescribed items with 49,844 unique descriptions. From the list of prescribed items 89.3% were mapped to the dm+d standard. 
-	There were 24,081,702 prescribed items where it was possible to compare the Medicines Text Mining Tool against manual dm+d mapping performed by Trusts. The results matched exactly 60.8% of the time and identified the same active ingredient (that is had a common VTM or VMP code) 99.3% of the time.
-	Assurances cannot be provided around the coverage of patient episodes nor bed days included in the data that was used to develop the "tool"
-	All reasonable endeavours have been undertaken to clinically assure the Medicines Text Mining Tool and reasonable attempts were undertaken to rectify issues and errors that were identified. 
-	A list of issues that have been identified and could not be rectified are provided in appendix A, please note that list is not comprehensive - there may be further issues and incorrect mappings that have yet been identified.  
-	Therefore, for reasons outlined above, NHS Digital cannot accept clinical responsibility for use of the "tool" and any outputs from use of the mapped data. The use of both the mapping tool and mapped data is the clinical responsibility of the user.


### APPENDIX A - Known issues with the Medicines Text Mining Tool
|No|Issue|Examples|
|--|--|--|
|1 |Differences in dm+d and ePMA naming conventions| <li> dm+d includes the salt name (sodium) in its naming of Levothyroxine sodium. If the ePMA term does not mention the salt name, the mapping tool may wrongly match it to liothyronine or leave it as unmappable </li><li> dm+d has 2 VTMs for phenytoin (phenytoin and phenytoin sodium). The ePMA naming convention affects which MTMT match is returned </li><li> The format of the ePMA term can affect the accuracy of the mapping. For example, the ePMA term 'Potassium Chloride (Sando-K) 12mmol K and 8mmol Cl Tablet' was matched to VTM Potassium chloride </li> |
|2	|Similar names may yield the wrong match|<li>ePMA term 'Trospium 20 mg Tablets' was matched to the VTM Chlordiazepoxide due to its association with AMPs with the brand name 'Tropium' </li><li>ePMA term 'Exorex cream' was matched to the VTM Coal tar due to its lexical similarity with 'Exorex lotion' which has a VTM of Coal tar</li><li>ePMA term 'OPTIUM (FREESTYLE) Test Strip' was matched to the VTM Opium </li><li>ePMA terms for 'Aquacel Ag' were matched to dm+d terms for 'Aquacel Ag+'</li><li>ePMA terms containing the word 'clear', for example 'RESOURCE THICKEN UP STAGE 1 = LEVEL 2 CLEAR POWDER' was matched to the VTM 'Docusate' due to the lexical similarity with the word 'clear' in the AMP term for the product 'Clear ear drop'</li><li>ePMA term 'FACTOR VIII 500iu/vWF 1200iu (VOCENTO) Injection' was matched to the VTM Factor XIII</li><li>ePMA terms for 'Hepatitis B vaccine' matched to the VTM Riboflavin due to the term 'Vitamin B'</li><li>ePMA term 'FLUPENTIXOL DECANOATE 100 mg/1 mL Injection' matched to VTM Flupentixol dihydrochloride</li>|
|3	|Multiple ingredient ePMA terms being matched inaccurately due to lexically similar dm+d entries|<li>ePMA terms for 'Meningococcal ACWY' were matched to dm+d terms for 'Meningococcal group C'</li><li>ePMA terms for 'Hepatitis A + B vaccine' were matched to dm+d terms for 'Hepatitis A vaccine' (and vice versa)</li><li>ePMA terms for 'diphtheria, pertussis, polio and tetanus vaccines' were matched to the dm+d terms for 'diphtheria, tetanus and pertussis'</li>|
|4	|ePMA typos	|<li>Misspelling of pyrazinamide as pyrizinamide. ePMA term 'Rifater (rifampicin, isoniazid and pyrizinamide) tablets' was matched to VTM Rifampicin + Isoniazid </li><li>Misspeling tafluprost as safluprost. ePMA term 'Safluprost + Timolol (Taptiqom) Unit Dose PF Eye Drops' was matched to VTM Timolol</li>|
|5	|Ambiguous ePMA term|<li>ePMA term 'MOVICOL / LAXIDO sachets' was unmappable</li>|
|6	|ePMA term contains additional pre/post-fixed information, abbreviations, additional spacing or brevity of the ePMA term|<li>ePMA term 'Adcal - D3 Caplet' matched to VTM Calcium carbonate</li><li>ePMA term 'TRELEGY ELLIPTA fluticasone 92mcg/umeclidinium 55mcg/vilante' matched to VTM Fluticasone</li>|
|7	|Closest lexical match is not the correct match	|<li>ePMA terms for 'phosphate enema' matched to the VTM Phosphate. The VMPs 'Phosphates enema (Formula B) 128ml long tube' and 'Phosphates enema (Formula B) 128ml standard tube' are linked to the VTM 'Sodium acid phosphate + Sodium phosphate'</li>|
|8	|Mismatching due to flavours|<li>ePMA term 'FORTISIP COMPACT PROTEIN (TROPICAL GINGER) Liquid' matched to VTM Ginger</li><li>ePMA term 'Peppermint water' matched to VTM Water</li>|
|9	|Non-medicinal product entries in ePMA|<li>ePMA terms such as 'drug not listed 1' and 'FLUIDS: See Paper Chart' were unmappable</li>|

# Getting started

This code runs on a Databricks cluster (Spark version 2.4.5) with the following packages:
	
- collections
- datetime
- enum
- functools
- operator
- os
- pandas
- pyspark.broadcast
- pyspark.sql
- random
- re
- time
- traceback
- typing
- unittest
- uuid
- warnings
        
The following dm+d tables are required as inputs to the pipeline:

- amp
- vmp
- vtm
- amp_parsed
- vmp_parsed
- form
- route
- unit_of_measure

# To run the pipeline
Locate and run the init_schemas notebook. You will need to specify the following, which are the database and table names where the outputs will be written to:
- db
- match_lookup_final_name
- unmappable_table_name
- accuracy_table_name

Locate and run the run_notebooks notebook. You will need to specify:
- source_dataset: Whether the input data is of type source_A or source_B
- raw_input_table: If input is of type source_B, this must contain string fields named "medication_name_value" and "form_in_text". If input is of the type source_A, this must contain a string field named "Drug"
- db: The database
- batch_size: The number of rows you'd like to process (i.e. The number of rows in raw_input_table)
- notebook_root: the location of this notebook
		
# Licence
Medicines Text Mining Tool codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the [Open Government 3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.
