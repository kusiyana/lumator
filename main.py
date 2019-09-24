# - - - - - - - - - - - - - - - - - - - - - - - -
# Luminator version 1.0
# Author: Hayden Eastwood
# email: heeastwo@amazon.lu
# last changed: 06/09/2019

# Description
# At the present moment Lumis is used to forecast carrier requirements for Turkey at the granularity of sort code, given the forecast of overall demand.
# Until recently, setting up simulations to achieve this was highly manual and time consuming. The Luminator automates this process and sends
# the output file (containing the breakdown of carriers and their respective sortcodes, for each predicted day) to a specified end user.
#In the future it’s hoped that Lumator can be adjusted to automate other Lumis tasks.
#
# In this script:
# 1. Read sql output from file indicating Turkish demand
# 2. adjust Lumis input file demand_changer.xls with input from 1
# 3. make other changes to parameter file
# 4. run lumis
# 5. extract output and email to emails in email_receivers.txt

# - - - - - - - - - - - - - - - - - - - - - - - -

# settings
# - - - - - - - - - - - - - - - - - - - - - - - -
days_ahead = 3
to = 'beastflow@gmail.com'
lumis_base_directory = '' # set this if you have your own folder to run Lumis from
# - - - - - - - - - - - - - - - - - - - - - - - -

# - - - - - - - - - - - - - - - - - - - - - - - -
# Code
#
import lumator
la = lumator.Lumis_automator(days_ahead)
la.simulation_title = 'TR-test-2'
if lumis_base_directory:
    la.lumis_base_directory = lumis_base_directory

#1. get demand
la.message('Lumator automation for Lumis. ')

la.get_demand()

#2. Get historical ship option groupings
la.write_lumis_demand_file()

#3. Build parameter file
la.write_lumis_parameter_file()

#4. Move demand and parameter files to Lumis
la.move_files_to_lumis()

#4. Run modified lumis
la.run_lumis()
la.message('Simulation end')
la.message('Extract results')
la.get_forecast_results()
la.write_forecast_file()
la.message('Send results to ' + to)
la.mail_results(to)

# - - - - - - - - - - - - - - - - - - - - - - - -
