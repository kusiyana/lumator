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
# - - - - - - - - - - - - - - - - - - - - - - - -

import xlsxwriter
import csv
import time, datetime
import psycopg2
import pandas as pd
import shutil
import os
import smtplib
from os.path import basename
from email.utils import formatdate
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email import encoders
from email.mime.base import MIMEBase

class Lumis_automator:

    # default variables
    delimiter = ' '                                                          # delimeter for writing data to text files
    FC = 'XTRA'                                                              # warehouse ID to perform demand simulation on
    forecast_output_filename = 'output/forecast_out.csv'
    SMTP_SERVER = 'smtp.amazon.com'
    units = 1                                                                # map pacakges to units
    lumis_columns = [                                                        # columns within lumis demand file
        'warehouse',
        'sog',
        'order_or_slam_day',
        'order_or_slam_month',
        'order_or_slam_year',
        'nb_packages',
        'units'
    ]
    #lumis_base_directory = '/Users/heeastwo/Documents/Luminator/Lumis-TR-beta'         # lumis base directory of installation
    lumis_input_directory = 'input/input_scenario/'                         # where lumis input files are stored
    demand_file_name = '__demand.txt'                                       # the temporary file name to store demand in
    mail_body_file = 'email_body.txt'                                       # the name of the file to provide the base email template

    #parameter file settings
    parameter_file_name = '__parameters.txt'                                # temporary file name for parameter file
    country_id='EU'                                                         # 2 letter country code
    run_id = ''                                                             # run ID for parameter file
    load_input_changers = "False"                                              # load input changers, True or False (text)
    activate_f2p_light = "False"                                            # activate f2p light, True of False (text)
    actuals_extraction_id = "DEFAULT"                                       # actuals extraction ID
    lumis_default_name = '/Lumis-TR-beta'

    # db settings
    host = 'eunp-sa-cluster.cv6bfrl69s9z.us-west-2.redshift.amazonaws.com'
    dbname='eunpsa'
    port='8192'
    user='eunpsateam1'
    password='Eunp2018!'
    #/ end DB settings

    def __init__(self, num_days_ahead, start_date = ''):
        csv.register_dialect('my_dialect', delimiter = self.delimiter, quoting=csv.QUOTE_NONE, skipinitialspace=True)
        self.dir_path = os.path.dirname(os.path.realpath(__file__))
        self.lumis_base_directory = self.dir_path + self.lumis_default_name + '/'
        self.demand_file_name = os.path.join(self.dir_path, self.demand_file_name)
        self.parameter_file_name = os.path.join(self.dir_path, self.parameter_file_name)
        now = time.strftime("%Y-%m-%d")
        self.simulation_title = 'TR-' + now
        self.num_days_ahead = num_days_ahead
        self.read_mail_default_body()
        if not start_date:
            self.start_date = now
        else:
            self.start_date = start_date



    def write_lumis_demand_file(self):
        """
        Write a demand file that Lumis can use for its simulation
        """
        # Create a workbook and add a worksheet.
        self.message('Writing demand file', 2)
        row_count = 0
        col = 0
        for row in self.demand.itertuples():
            self.message('Writing row ' + str(row_count), 3)
            col_count=0
            if row_count == 0:
                # write column headings
                with open(self.demand_file_name, 'w') as csv_file:
                    csv_file.write('\t'.join(self.lumis_columns[0:]) + '\n')
                    self.message('getting SOGS for ' + str(row[4])[0:10], 3)
            sogs = self.db_get_sog(str(row[4])[:10]) # get the sogs for given date

            with open(self.demand_file_name, 'a') as csv_file:
                for i in range(0, sogs.shape[0]):
                    self.message ('unpacking for ' + str(sogs['ship_option_group'][i]), 3)
                    nb_packages = int(round(row[8] * sogs['p'][i]))
                    demand_list=[
                        str(row[5])
                        ,str(sogs['ship_option_group'][i])
                        ,str(self.date(str(row[4]), 'd'))
                        ,str(self.date(str(row[4]), 'm'))
                        ,str(self.date(str(row[4]), 'y'))
                        ,str(nb_packages)
                        ,str(  int(round(self.units * nb_packages)))
                    ]
                    csv_file.write('\t'.join(demand_list[0:]) + '\n')
                    row_count+=1
        csv_file.close()

    def date(self, date_string, format):
        """
        Break down date into day, month and year for Lumis demand input file
        Arguments:
            date_string:    eg '3/23/19' (ie mm/dd/yy)
            format:         'd' or 'm' or 'y'

        Output:             day, month or year integer
        """
        date_string = date_string[:10]
        if format.lower() == 'd':
            format_string = '%d'
        elif format.lower() == 'm':
            format_string = '%m'
        elif format.lower() == 'y':
            format_string = '%Y'
        date_output = datetime.datetime.strptime(date_string, '%Y-%m-%d').strftime(format_string)
        return date_output


    def get_demand(self):
        """
        Get demand data for given warehouse in given territory

        Arguments:      days_back - number of days to look back in time

        output:         self.demand written with demand data, True result returned
        """

        self.message('Getting demand data', 2)
        demand_query = """
        SELECT
            region
            ,org
            ,forecast_date
            ,target_date
            ,fc
            ,flow
            ,metric_name
            ,metric_value
        FROM eunpsa.daily_forecast_tr
        WHERE target_date between CURRENT_DATE and CURRENT_DATE + """ + str(self.num_days_ahead) + """
        AND metric_name='Forecasted Customer Shipments'
        GROUP BY 1,2,3,4,5,6,7,8
        """
        self.db_connect()
        query_result = self.db_query(demand_query)
        self.demand = query_result
        return True


    def move_files_to_lumis(self):
        """
        Move files from running directory to the specified lumis directory
        """
        self.message('Moving files to Lumis', 2)
        input_directory = self.lumis_base_directory + self.lumis_input_directory
        #try:
        self.message('Moving demand.txt', 3)
        if not os.path.isdir(input_directory):
            os.mkdir(input_directory)
        shutil.copy(self.demand_file_name, input_directory + 'demand.txt')
        self.message('Moving parameters.txt', 3)
        shutil.copy(self.parameter_file_name, self.lumis_base_directory + 'parameters.txt')
        #except:
        ##    print ("ERROR: could not move all generated files to Lumis input folder!")
        #    exit()
        return True


    # Private functions
    def db_connect(self):
        """
        Connect to Database
        """
        self.conn = psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            port=self.port,
            user=self.user,
            password=self.password
            )

    def db_query(self, query_string):
        """
        Run generic database query for query_string
        """
        query_result = pd.read_sql(query_string, self.conn)
        return query_result

    def db_get_sog(self, date):
        """
        Generate ship options group (SOG) query
        """
        sog_sample_date = self.get_sog_sample_date(date)
        self.message('Sampling historic date: ' + str(sog_sample_date) + ' (' + datetime.datetime.strptime(sog_sample_date, "%Y-%m-%d").strftime('%A') + ')' , 3)
        query = """
        SELECT
            CASE
                WHEN sog.group_name = 'PREMIUM-SAME' then 'SAME'
                WHEN sog.group_name = 'PREMIUM-NEXT' then 'PREMIUM'
                WHEN sog.group_name = 'STANDARD' then 'STANDARD'
                WHEN sog.group_name = 'ECONOMY' then 'ECONOMY'
                WHEN sog.group_name = 'PREMIUM-TWO' then 'PREMIUM-TWO'
            END AS SHIP_OPTION_GROUP
            ,count(distinct fulfillment_shipment_id||package_id) as sub_total
        FROM bits.d_outbound_ship_items_eu osp
        LEFT JOIN  bits.nship_method_groupings smg ON osp.pkg_ship_method  = smg.ship_method
            AND osp.region_id = smg.region
            AND osp.legal_entity_id = smg.legal_entity_id
        LEFT JOIN  bits.nship_option_groupings sog ON (osp.ordering_ship_option = sog.ship_option
            AND osp.region_id = sog.region
            AND osp.legal_entity_id = sog.legal_entity_id)
        WHERE osp.warehouse_id = '""" + self.FC + """ '
            AND OSP.ship_day = '""" + str(sog_sample_date) + """'
            AND sog.group_type_name in ('SHIP_OPTION_GROUP_OB')
            AND OSP.legal_entity_id = 141
        GROUP BY 1
        """
        self.db_connect()
        query_result = self.db_query(query)
        total = query_result.sum(axis=0)[1]
        query_result['p'] = query_result['sub_total']/total
        return query_result

    def db_get_lumis_result(self):
        """
        Get Lumis results from lumis database
        """
        query = """
        SELECT
            sog.group_name AS SHIP_OPTION_GROUP
            ,osp.ordering_ship_option
            ,count(distinct fulfillment_shipment_id||package_id) as sub_total
        FROM bits.d_outbound_ship_items_eu osp
        LEFT JOIN  bits.nship_method_groupings smg ON osp.pkg_ship_method  = smg.ship_method
            AND osp.region_id = smg.region
            AND osp.legal_entity_id = smg.legal_entity_id
        LEFT JOIN  bits.nship_option_groupings sog ON (osp.ordering_ship_option = sog.ship_option
            AND osp.region_id = sog.region
            AND osp.legal_entity_id = sog.legal_entity_id)
        WHERE osp.warehouse_id = '""" + self.FC + """ '
            AND OSP.ship_day = '""" + str(date) + """' - 7
            AND sog.group_type_name in ('SHIP_OPTION_GROUP_OB')
            AND OSP.legal_entity_id = 141
        GROUP BY 1,2
        """
        self.db_connect()

    def get_forecast_results(self):
        """
        Make a call to lumis.output_raw to extract the final projections.
        SQL is generated for desired dates and then executed
        """
        test = self.num_days_ahead
        start_date_obj = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").date()
        query_base = """
        select
        carrier
        ,sort_code
        ,SUBSTRING(cpt_datetime::varchar(25), 11,6) as cpt_time
        \n
        """
        query_end = """
        FROM lumis.output_raw
        WHERE simulation_title='""" + self.simulation_title + """'
        AND scenario_id='scenario'
        GROUP BY 1,2,3
        """
        query_middle = ""
        day = 1
        for single_date in (start_date_obj + datetime.timedelta(day) for day in range(self.num_days_ahead)):
            query_middle+=",sum(case when trunc(cpt_datetime) = '" + str(single_date) + "' then nb_packages end) as date_" + str(single_date).replace('/','_').replace('-','_') + "\n"
            day+=1
        query = query_base+query_middle+query_end
        self.db_connect()
        query_result = self.db_query(query)
        self.forecast = query_result
        print (self.forecast)

    def write_forecast_file(self):
        self.forecast.to_csv(self.forecast_output_filename)


    def write_lumis_parameter_file(self):
        """
        Write the Lumis parameter file
        """
        self.message('Writing Lumis parameter file', 2)

        file = open(self.parameter_file_name, 'w')
        file.write('simulation title \t' + self.simulation_title + "\n")
        file.write('run_id \t' + str(self.run_id)  + "\n")
        file.write('country ID (GB,DE,FR,IT,ES) \t' + str(self.country_id)  + "\n")
        file.write('Run date \t' + "\n")
        file.write('Actuals extraction id  \t' + self.actuals_extraction_id + "\n")
        file.write('Load input changers ? (True/False) \t' + self.load_input_changers  + "\n")
        file.write('Activate F2P light ? (True/False) \t' + self.activate_f2p_light)
        file.close()

    def run_lumis(self):
        """
        Run Lumis base and scenario
        """
        self.message('Running Lumis baseline: ' + "/anaconda3/bin/python " + self.lumis_base_directory + "Lumis.py")
        os.chdir(self.lumis_base_directory)
        os.system("/anaconda3/bin/python " + self.lumis_base_directory + "Lumis.py")
        self.message('Running Lumis scenario')
        os.system("/anaconda3/bin/python " + self.lumis_base_directory + "Lumis_scenario.py")



    def message(self, message, level=1):
        """
        Print informative message
        """
        if level == 1:
            print ( ' ***** ' + str(message) + ' ***** ' )
        if level == 2:
            print ('     -- ' + str(message))
        if level == 3:
            print ('           - ' + str(message))


    def get_sog_sample_date(self, demand_date):
        """
        Return date to perform sampling on

        Input:  demand_date (contains the date of demand that historic values are needed for)
        Output: date at which sampling should be done
        """
        day_demand = datetime.datetime.strptime(demand_date, "%Y-%m-%d").weekday() # day of demand
        day_today = datetime.datetime.now().weekday()
        most_recent_day = day_demand - day_today - 7
        date_for_sog = (datetime.datetime.now() + datetime.timedelta(days=most_recent_day)).strftime("%Y-%m-%d")
        return (date_for_sog)


    def read_mail_default_body(self):
        """
        Read mail from the default body of
        """
        try:
            file = open(self.mail_body_file, 'r')
            email_body = file.read()
        except:
            self.message('Could not read email body, using default', 3)
            email_body = 'Please find the forecast file attached'
        self.email_body = email_body


    def create_email_message(self, from_addr, to_addr, cc_addr, bcc_addr, subject, body, fp):
        msg = MIMEMultipart()
        msg.attach(MIMEText(body))
        msg['Subject'] = subject
        msg['From'] = from_addr
        msg['To'] = ",".join(to_addr)
        msg['Cc'] = ",".join(cc_addr)
        msg['Bcc'] = ",".join(bcc_addr)
        msg['Date'] = formatdate()

        # ADD_ATTACHMENT
        path = fp
        if len(path) != 0:
            for file in path:
                with open(file, "rb") as f:
                    msg2 = MIMEBase('application', "octet-stream")
                    msg2.set_payload(f.read())
                    encoders.encode_base64(msg2)
                    msg2.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file))
                msg.attach(msg2)

        return msg


    def send_email(self, FROM, TO, CC, BCC, msg, SMTP):
        SEND = [",".join(TO), ",".join(CC), ",".join(BCC)]
        smtpobj = smtplib.SMTP(SMTP, 25)
        smtpobj.ehlo()
        smtpobj.starttls()
        smtpobj.ehlo()
        # smtpobj.login('ant/mikohei', "MY_PASSWORD")
        smtpobj.sendmail(FROM, SEND, msg.as_string())
        smtpobj.close()


    def mail_results(self, to):

        mail = {
            'From': 'heeastwo@amazon.lu',
            'To': [to],
            'Cc': [],
            'Bcc': [],
            'Subject': 'Lumis Forecast',
            'Body': "\n".join([
                self.email_body,
            ]),
            'Attach': [self.forecast_output_filename
            ]
        }
        message = self.create_email_message(mail['From'], mail['To'], mail['Cc'], mail['Bcc'],
                                 mail['Subject'], mail['Body'], mail['Attach'])
        self.send_email(mail['From'], mail['To'], mail['Cc'], mail['Bcc'], message, self.SMTP_SERVER)
