#!/usr/bin/env python
# coding: utf-8

def check_spark_profile_install():
    if 'spark_df_profiling' not in dir():
        try:
            get_ipython().system('python3.8 -m pip install --no-deps --user ./spark-df-profiling-master.zip')
            import spark_df_profiling
        except Exception as e:
            pass
                      
            
def main(df):
    check_spark_profile_install()
    import spark_df_profiling
    df = df.sample(.1)
    df = df.cache()
    return spark_df_profiling.ProfileReport(df)


