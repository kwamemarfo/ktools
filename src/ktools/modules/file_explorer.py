#!/usr/bin/env python
# coding: utf-8


import ipywidgets as widgets
import pandas as pd

from IPython.display import HTML
from IPython import get_ipython

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_colwidth', None)

class File_Explorer():
    out = widgets.Output(layout={'border': '1px solid black'})
    
    
    def remove_backslash(self, name):
        if "\\" in name:
            name = name.replace("\\", "")
        return name

    def find_req(self, search_term):
        display("Searching ....")
        search_term = f"{search_term}"    
        req_dir = get_ipython().getoutput('find /home/jupyterhub/ -type f -user $search_term -ls 2>&1         | grep -v "Permission denied"')
        
        all_info = []   
        try:
            for info in req_dir:
                location = f"/home/{info.split('/home/')[-1]}"
                info = info.split("   ")
                info = [x for x in info if x != '']
                user_name = info[-2].split(" ")[-1]
                daten = " ".join(info[-1].lstrip().rstrip().split(" ")[1:4])
                file_name = info[-1].split("/")[-1]
                file_name = self.remove_backslash(file_name)
                location = self.remove_backslash(location)
                folder_name = "/".join(location.split("/")[4:])
                if "/" in folder_name:
                    folder_name = folder_name.split("/")[-2]
                else:
                    folder_name = "/home"
                if not folder_name.startswith("."):    
                    collected_data = user_name, daten, folder_name, file_name, location
                    all_info.append(collected_data)
        except Exception as e:
                self.out.clear_output()
                print("User not found")
                return None
        self.out.clear_output()
        return pd.DataFrame(all_info, columns=["User_ID", "Date", "Folder_Name", "File_Name", "Location"])



    def data_search(self, staffid):
        self.data_found = self.find_req(staffid)
        if self.data_found is not None:
            self.data_found = self.data_found.sort_values(by=["Date"], ascending=False)
            self.data_found = self.data_found.reset_index().drop('index', axis=1)
            self.data_found.rename_axis("index", axis=1, inplace=True)
        return self.data_found
        

    def convert_notebook_to_html(self, notebook_location):
        notebook_location = f"'{notebook_location}'"
        std_notebook_to_html = get_ipython().getoutput('jupyter nbconvert --stdout $notebook_location / --to html')
        html_notebook_display = "\n".join(str(item) for item in std_notebook_to_html[1:])
        html_notebook_display = f"{html_notebook_display.split('</html>')[0]}</html>"
        html_notebook_display = HTML(data=html_notebook_display)
        return html_notebook_display
    
    def convert_file_location_to_pd_df(self, Filepath, fileExtension):
        try:
            if fileExtension == 'xlsx':
                load_file = pd.read_excel(Filepath)
            elif fileExtension == 'csv':
                load_file = pd.read_csv(Filepath)
            elif fileExtension == 'json':
                load_file = pd.read_json(Filepath)
            else:
                load_file = HTML(filename=Filepath)
            load_file = load_file
        except Exception as e:
            load_file = f"Problems with this filepath and/or file \n --- {e}"
        return load_file
    

    def files_supported(self, filepath):
        file_ext = filepath.split(".")[-1]
        pd_support = ['xlsx', 'csv', 'json', 'html']
        if file_ext == 'ipynb':
            support_file = self.convert_notebook_to_html(filepath)
        elif file_ext in pd_support:
            support_file = self.convert_file_location_to_pd_df(filepath, file_ext)
        else:
            support_file = 'File Not Supported'
        return support_file
    

    def copy_file_from_df_location(self, df):
        file_location = str(df["Location"].iloc[0])
        file_location = self.remove_backslash(file_location)
        userid = str(df["User_ID"].iloc[0])

        fileName_with_userid = file_location.split("/")[-1]
        if "." in fileName_with_userid:
            fileName_with_userid = fileName_with_userid.split(".")
            fileName_with_userid =             f"{fileName_with_userid[0]}_From_UserID_{userid}.{fileName_with_userid[-1]}"
        else:
            fileName_with_userid = f"{fileName_with_userid}_{userid}"
        fileName_with_userid = f"'./{fileName_with_userid}'"

        file_location = f"'{file_location}'"
        copy_file = get_ipython().getoutput('cp $file_location $fileName_with_userid')
        if not len(copy_file):
            copy_file = f"{file_location} --- Copied to --> {fileName_with_userid}"
        else:
            copy_file = f"Failed to copy file : {file_location}"
        return str(copy_file)
    
    
    
    def copy_file_from_df_location_with_no_output(self, df):
        file_location = str(df["Location"].iloc[0])
        file_location = self.remove_backslash(file_location)
        userid = str(df["User_ID"].iloc[0])

        fileName_with_userid = file_location.split("/")[-1]
        fileName_with_userid = fileName_with_userid
        if "." in fileName_with_userid:
            fileName_with_userid = fileName_with_userid.split(".")
            fileName_with_userid =             f"{fileName_with_userid[0]}_From_UserID_{userid}.{fileName_with_userid[-1]}"
        else:
            fileName_with_userid = f"{fileName_with_userid}_{userid}"
        fileName_with_userid = f"'./{fileName_with_userid}'"

        file_location = f"'{file_location}'"
        copy_file = get_ipython().getoutput('cp $file_location $fileName_with_userid')

        if "ipynb" in fileName_with_userid:
            nn = get_ipython().getoutput('jupyter nbconvert --clear-output --inplace $fileName_with_userid')
        if not len(copy_file):
            copy_file = f"{file_location} --- Copied to --> {fileName_with_userid}"
        else:
            copy_file = f"Failed to copy file : {file_location}"
        return str(copy_file)
    
    
    def check_visibility(self):
        try:
            if self.data_found is not None and             self.comboBox_requestSelect.value in self.comboBox_requestSelect.options and             self.comboBox_folderSelect.value in self.comboBox_folderSelect.options:
                self.button_view.layout.visibility = 'visible'
                self.button_copy.layout.visibility = 'visible'
                self.button_copy2.layout.visibility = 'visible'
            else:
                self.button_view.layout.visibility = 'hidden'
                self.button_copy.layout.visibility = 'hidden'
                self.button_copy2.layout.visibility = 'hidden'
                
        except:
            pass
    

    def all_widgets(self):
        Layout = widgets.Layout
        
        self.comboBox_userSelect = widgets.Combobox(
            placeholder='Enter Staff ID',
            description='Staff ID:',
            layout=Layout(width='auto', margin='40px 10px 10px 0')
        )

        self.comboBox_requestSelect = widgets.Combobox(
            placeholder='Select a File',
            description='File Name:',
            layout=self.comboBox_userSelect.layout
        )

        self.comboBox_folderSelect = widgets.Combobox(
            placeholder='Select a Folder',
            description='Folder Name:',
            layout=self.comboBox_userSelect.layout
        )

        self.button_view = widgets.Button(
            description='View File',
            button_style='info',
            icon='eye', 
            layout=Layout(margin='10px 10px 25px 100px', width='auto')
        )
        self.button_view.layout.visibility = 'hidden'

        self.button_copy = widgets.Button(
            description='Copy File',
            button_style='info',
            icon='copy', 
            layout=Layout(margin='10px 10px 25px 100px', width='auto')
        )
        self.button_copy.layout.visibility = 'hidden'

        self.button_copy2 = widgets.Button(
            description='Copy File Without Output',
            button_style='info',
            icon='copy', 
            layout=Layout(margin='10px 10px 25px 100px', width='auto')
        )
        self.button_copy2.layout.visibility = 'hidden'
        
        
    @out.capture(clear_output = True)
    def comboBox_userSelect_eventhandler(self, change):
        user_data = self.data_search(change.new)
        if user_data is not None:
            self.comboBox_folderSelect.options = user_data.Folder_Name.unique().tolist()
            self.comboBox_requestSelect.options = user_data.File_Name.tolist()
        else:
            self.comboBox_folderSelect.options = [""]
            self.comboBox_requestSelect.options = [""]
        self.check_visibility()
            
        
    def comboBox_folderSelect_eventhandler(self, change):
        if self.data_found is not None:
            if change.new == '':
                self.comboBox_requestSelect.value = ''
                self.folder_data = self.data_found.File_Name.tolist()
            else:
                self.folder_data = self.data_found[(self.data_found["Folder_Name"] == 
                                                    change.new)].File_Name.tolist()
            self.comboBox_requestSelect.options = self.folder_data
            self.check_visibility()
            

    @out.capture(clear_output = True)
    def comboBox_requestSelect_eventhandler(self, change):
        if change.new in self.comboBox_requestSelect.options:
            if self.comboBox_folderSelect.value == '':
                self.df_file = self.data_found[(self.data_found["File_Name"] == 
                                                    change.new)].Folder_Name.tolist()
                self.comboBox_folderSelect.value = self.df_file[0]
        self.check_visibility()
        
        
    @out.capture(clear_output = True)
    def button_view_eventhandler(self, click):
        data_to_display = self.data_found[((self.data_found["File_Name"] == self.comboBox_requestSelect.value) & 
                                           (self.data_found["Folder_Name"] == self.comboBox_folderSelect.value))]
        display(self.files_supported(data_to_display["Location"].values[0]))
            

    @out.capture(clear_output = True)
    def button_copy_eventhandler(self, click):
        data_to_display = self.data_found[((self.data_found["File_Name"] == self.comboBox_requestSelect.value) & 
                                           (self.data_found["Folder_Name"] == self.comboBox_folderSelect.value))]
        display(self.copy_file_from_df_location(data_to_display))
        
        

    @out.capture(clear_output = True)
    def button_copy_eventhandler2(self, click):
        data_to_display = self.data_found[((self.data_found["File_Name"] == self.comboBox_requestSelect.value) & 
                                           (self.data_found["Folder_Name"] == self.comboBox_folderSelect.value))]
        display(self.copy_file_from_df_location_with_no_output(data_to_display))
        
     
    
    def observations(self):
        self.comboBox_userSelect.observe(self.comboBox_userSelect_eventhandler, names="value")
        self.comboBox_requestSelect.observe(self.comboBox_requestSelect_eventhandler, names="value")
        self.comboBox_folderSelect.observe(self.comboBox_folderSelect_eventhandler, names="value")
        self.button_view.on_click(self.button_view_eventhandler)
        self.button_copy.on_click(self.button_copy_eventhandler)
        self.button_copy2.on_click(self.button_copy_eventhandler2)
    
    def main_code(self):
        self.all_widgets()
        self.observations()
        Layout = widgets.Layout
        box_layout = Layout(display='flex',
                            flex_flow='column',
                            align_items='stretch',
                            #border='solid',
                            width='33%')

        style = {'description_width': 'initial'}
        intText_indexSelect = widgets.IntText(description='index (Overrides above selection):', 
                                              layout=Layout(margin='10px 10px 25px 10px', width='auto'), style=style)

        
        left_box = widgets.VBox([self.comboBox_userSelect, self.button_view], layout=box_layout)
        middle_box = widgets.VBox([self.comboBox_folderSelect, self.button_copy], layout=box_layout)
        right_box = widgets.VBox([self.comboBox_requestSelect, self.button_copy2], layout=box_layout)
        box = widgets.HBox([left_box, middle_box, right_box], layout={'border': '1px solid black'})

        te_b = widgets.HBox([self.out], layout={'margin': '25px 0 0 0'})
        fin = widgets.VBox([box, te_b], layout={'border': '2px solid gray'})
        
        display(fin)
        


