#!/usr/bin/env python
# coding: utf-8

import ipywidgets as widgets
import requests, re
import pandas as pd
from ktools.modules import pyspark_profile, kt

import IPython

class Parquet_Dashobard:
    def __init__(self, usr, ss):
        self.user_name = usr
        self.ss = ss
        self.main_link = "http://gbl20174697:12000/v4/"
        self.batches_link = "batches"
        self.assets_link = "assets/%s/index"
        self.out = widgets.Output()
        
           
    def get_page(self, link, *data):
        s = requests.session()
        headers = {'X-Data-Refinery-Client-Id': self.user_name, 'X-Data-Refinery-Consumer-Type':                        'INDIVIDUAL_USER', 'Content-Type': 'application/json'}
        s.headers.update(headers) 
        if data:
            page = s.post(link, data=data[0])
        else:
            page = s.get(link).json()
        return page 

    def get_batches(self):
        batches = self.get_page(f"{self.main_link}{self.batches_link}")["snapshots"]
        batches = [x["runId"] for x in batches if '-' in x["runId"]]
        batches.sort(reverse=True)
        batches.insert(0, " ")
        return batches
    
    
    def get_assets(self, batchId):
        link = self.assets_link % (batchId)
        link = f"{self.main_link}{link}"
        self.assets = self.get_page(link)
        return self.assets
    
    def get_Source(self, batchId):
        if batchId != '':
            assets = self.get_assets(batchId)
            sources = [x for x in assets]
            if "runId" in sources:
                sources.remove("runId")
            sources = sources
        else:
            sources = []
        return sources
    
    def get_assetList(self, sourceId):
        if sourceId:
            try:
                asset_list = self.assets[sourceId]
                if type(asset_list[0]) == dict:
                    self.assets_pd = pd.DataFrame(asset_list)
                    asset_list = self.assets_pd["asset"].tolist()
            except Exception as e:
                asset_list = []
                
        else:
            asset_list = []    
        return asset_list
    
    
    def check_selection(self):
        if self.comboBox_batchSelect.value in self.comboBox_batchSelect.options:
            if self.comboBox_sourceSelect.value in self.comboBox_sourceSelect.options:
                if self.comboBox_assetSelect.value in self.comboBox_assetSelect.options:
                    return True
        return False
    
    def get_asset(self, *partitions):
        message = "Please Select from items listed"
        link = f"{self.main_link}assets/{self.comboBox_batchSelect.value}"

        if self.check_selection():
            if self.comboBox_sourceSelect.value == "mdas":
                asset = self.assets_pd.loc[self.assets_pd["asset"] == self.comboBox_assetSelect.value]
                asset = asset.values.tolist()[0]
                data = '{"%s" : [{"source": "%s", "asset": "%s", "includePartitions": false}]}' %                 (self.comboBox_sourceSelect.value, asset[1], asset[2])
                if partitions:
                    data = '{"%s" : [{"source": "%s", "asset": "%s", "includePartitions": %s}]}' %                     (self.comboBox_sourceSelect.value, asset[1], asset[2], "true")

                data = self.get_page(link, data).json()
                if partitions:
                    message = ''
                    link = data['mda']['assets'][0]['path']
                    partition_list = data['mda']['assets'][0]['partitions']
                    partition_list = re.findall(f"'subPath': '(.*?)'", str(partition_list))
                    self.comboBox_partitionSelect.options = partition_list
                else:
                    parq_path = data['mda']['assets'][0]['path']
                    message = self.generate_spark_parquet(parq_path)

            elif self.comboBox_sourceSelect.value == "gdas":
                data = '{"gdas": [{"name": "%s"}]}' % (self.comboBox_assetSelect.value)
                data = self.get_page(link, data).json()
                parq_path = data['gda']['assets'][0]['gdaPath']
                message = self.generate_spark_parquet(parq_path)     

            else:
                message = (f"{self.comboBox_sourceSelect.value} not supported")

        self.message = message
        return message
    
    
    def generate_spark_parquet(self, parquet_path):
        generated_spark_parquet = parquet_path
        try:
            generated_spark_parquet = self.ss.read.parquet(parquet_path)
            kt.parq_file = generated_spark_parquet
            self.out.clear_output()
            generated_spark_parquet = f"Spark generated with variable (kt.parq_file) \nkt.parq_file = spark.read.parquet('{parquet_path}')"
        except NameError:
            generated_spark_parquet = f"Please Authenticate to enable loading of parquet file:\n {parquet_path}"
        except AttributeError:
            generated_spark_parquet = f"Please Authenticate to enable loading of parquet file:\n {parquet_path}"
        except Exception as e:
            generated_spark_parquet = e
            
        return generated_spark_parquet
        
        
    
    def comboBox(self):
        self.comboBox_batchSelect = widgets.Dropdown(
            placeholder='Choose a Date',
            options= self.get_batches(),
            description='Date:'
            )
        
        self.comboBox_sourceSelect = widgets.Dropdown(
            placeholder='Choose a Source',
            description='Source:'
        )
        
        self.comboBox_assetSelect = widgets.Combobox(
            placeholder='Choose a Parquet File',
            description='Asset:'
        )
        
        self.button_loadBtn = widgets.Button(
            description='Load Parquet',
            button_style='info',
            icon='eye',
            layout={'margin': '0 60px 0 50px'}
        )   
        
        self.button_generateBtn = widgets.Button(
            description='Generate Code',
            button_style='info',
            icon='eye'
        )
        
        self.checkBox_partitionsCheck = widgets.Checkbox(
            value=False, 
            description="Include Partitions", 
            disabled=False
        )
        self.checkBox_partitionsCheck.layout.visibility = 'hidden'
        
        
        self.comboBox_partitionSelect = widgets.Combobox(
            placeholder='Choose a Partition Country & Date',
            description='Partition:'
        )
        self.comboBox_partitionSelect.layout.visibility = 'hidden'
            
            
        self.button_generateProfile = widgets.Button(
            description='Analyse',
            button_style='',
            icon='eye'
        )
        

    def comboBox_batchSelect_eventhandler(self, change):
        self.comboBox_sourceSelect.options = self.get_Source(change.new)
        if self.comboBox_sourceSelect.value != '':
            self.comboBox_assetSelect.options = self.get_assetList(self.comboBox_sourceSelect.value)
        
    def comboBox_sourceSelect_eventhandler(self, change):
        self.checkBox_partitionsCheck.layout.visibility = 'hidden'
        self.comboBox_assetSelect.options = self.get_assetList(change.new)
        if change.new == 'mdas' and self.comboBox_assetSelect.value != '':
            self.checkBox_partitionsCheck.layout.visibility = 'visible'
            self.checkBox_partitionsCheck.value = False
        
    def comboBox_assetSelect_eventhandler(self, change):
        self.checkBox_partitionsCheck.layout.visibility = 'hidden'
        if self.check_selection and change.new != '' and self.comboBox_sourceSelect.value == 'mdas':
            self.checkBox_partitionsCheck.layout.visibility = 'visible'
            self.checkBox_partitionsCheck.value = False            
        
        
    def button_loadBtn_eventhandler(self, click):
        if all([self.comboBox_batchSelect.value, self.comboBox_sourceSelect.value,             self.comboBox_assetSelect.value]):
            link = self.get_asset()
            self.for_output(link)
            return link
       # return 
    
    def checkBox_partitionsCheck_eventhandler(self, change):
        self.for_output(self.get_asset("true"))
        if change.new == True:
            self.comboBox_partitionSelect.layout.visibility = 'visible'
        else:
            self.comboBox_partitionSelect.layout.visibility = 'hidden'
    
    
    def button_generateBtn_eventhandler(self, click):
        if all([self.comboBox_batchSelect.value, self.comboBox_sourceSelect.value,             self.comboBox_assetSelect.value]):
            self.out.clear_output()
            generate_code = f"""Use the following code :
k_parq = ss.read.parquet(get_parquet('{self.comboBox_batchSelect.value} {self.comboBox_sourceSelect.value} {self.comboBox_assetSelect.value}'))
            
Note: The code above is only available in the installed version, alternatively use:
k_parq = ktools.get_latest_parq('{self.comboBox_assetSelect.value}')
            """
            self.for_output(generate_code)
            
    def button_generateProfile_eventhandler(self, click):
        if all([self.comboBox_batchSelect.value, self.comboBox_sourceSelect.value, self.comboBox_assetSelect.value]):
            if self.ss != ' ':
                ## Resolve This import later !!!!!!!!!!!!!!!!!
                import pyspark
                if not isinstance(kt.parq_file, pyspark.sql.dataframe.DataFrame):
                    parq_link = self.button_loadBtn_eventhandler('Click')
                    parq_link = parq_link.split("hdfs")[-1]
                else:
                    parq_link = self.message.split("hdfs")[-1]
                    parq_link = f"hdfs{parq_link}"
                    
                self.for_output(parq_link)
                if parq_link:
                    try:
                        py_profile = pyspark_profile.main(kt.parq_file)
                        self.for_output(parq_link)
                        display(py_profile)
                        kt.py_profile = py_profile
                    except ModuleNotFoundError:
                        self.for_output(f"Module install failed: {parq_link}")
            else:
                self.for_output(f"Please Authenticate to analyse the file -- { dir()}")
        else:
            self.for_output("Please select All fields to enable analysis")
        
            
        
    def for_output(self, message):
        self.out.clear_output()
        with self.out:
            if type(message) == "pyspark.sql.dataframe.DataFrame":
                return message
            else:
                print(message)
                return message


    def observe(self):
        self.comboBox_batchSelect.observe(self.comboBox_batchSelect_eventhandler, names="value")
        self.comboBox_sourceSelect.observe(self.comboBox_sourceSelect_eventhandler, names="value")
        self.comboBox_assetSelect.observe(self.comboBox_assetSelect_eventhandler, names="value")
        self.button_loadBtn.on_click(self.button_loadBtn_eventhandler)
        self.checkBox_partitionsCheck.observe(self.checkBox_partitionsCheck_eventhandler, names="value")
        self.button_generateBtn.on_click(self.button_generateBtn_eventhandler)
        self.button_generateProfile.on_click(self.button_generateProfile_eventhandler)
    
    
            
    def main_code(self):
        Layout = widgets.Layout
        self.comboBox()
        self.observe()
        top_widgets = widgets.HBox([self.comboBox_batchSelect, self.comboBox_sourceSelect, self.comboBox_assetSelect], 
                                   layout={'margin': '10px 0 0 0'})
        middle_widgets = widgets.HBox([self.button_loadBtn, self.button_generateBtn, self.checkBox_partitionsCheck, 
                                       self.button_generateProfile], layout={'margin': '15px 0 0 0'})
        
        te_b = widgets.HBox([self.out], layout={'margin': '25px 0 0 0'})
        combined_widgets = widgets.VBox([top_widgets, middle_widgets, self.comboBox_partitionSelect, te_b])
        display(combined_widgets)
        





