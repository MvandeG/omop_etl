#!/usr/bin/env python3
"""
Observational health data & Genomic variations ETL for OMOP Postgresql DB

Usage: app.py sample.pdf sample.vcf 
"""

__author__ = "Glenn Hulscher & Milo van de Griend"
__version__ = "1.0.0"
__license__ = "MIT"

import sys
import subprocess
import re
import tabula
import pandas as pd
import psycopg2 as pg
from tkinter import filedialog as fd

db_conn = pg.connect(host="localhost", port="5432", dbname="postgres", user="postgres", password="password")
db_cursor = db_conn.cursor()

def pdfparser(pdf):
    """
    Read data from PDF and output to CSV format.
    
    Keyword arguments:
    pdf -- absolute/relative filepath of PDF
    """
    csv = pdf.rstrip(".pdf")+".csv"
    tabula.convert_into(pdf, csv, pages="1")
    dataString = str(tabula.read_pdf(pdf, pages='2')[0])
    print(dataString)
    print(pdf)
    header = dataString.split("\n")[0].split("Participant")[1]
    rest = dataString.split("\n")[1].split(pdf.split("/")[-1].rstrip(".pdf"))[1]
    
    df = pd.read_csv(csv)
    df[header] = rest
    df.to_csv(csv, index=False)
    return csv

def vcffilter(vcf,csv):
    """
    Read data from VCF and output 10 frameshift or missense variants to CSV format.
    
    Keyword arguments:
    pdf -- absolute/relative filepath of PDF
    """
    id = csv.rstrip(".csv")
    count = 0
    input = open(vcf, "r")
    chr21 = open("{}_chr21.vcf".format(id),"w")
    chr21_ann_flt_10 = open("{}_chr21_ann_flt_10.csv".format(id), "w")
    for line in input:
        if line.startswith("#") or line.startswith("chr21"):
            chr21.write(line)
    input.close()
    chr21.close()
    print(id)
    snpeff = """java -Xmx4g -jar "snpEff.jar" GRCh37.75 -t {0}_chr21.vcf > {0}_chr21_ann.vcf""".format(id)
    print(snpeff)
    subprocess.call(snpeff,shell=True)
    ann = open("{}_chr21_ann.vcf".format(id),"r")
    print(ann)
    for line in ann:
        if line.find("frameshift") != -1 or line.find("missense") != -1:
            print(line)
            chr21_ann_flt_10.write(re.sub('\t',',',re.sub('(^|[\t])([^\t]*\,[^\t\n]*)', r'\1"\2"', line)))
            count+=1 
        print(count)
        if count == 10:
                    break
    ann.close()
    chr21_ann_flt_10.close()
    
def postgres():
    """Browse OMOP-conformed CSV file(s) and load the data into postgresql DB."""
    filenames = fd.askopenfilenames(title = "Open csv file(s) for insertion to postgresql database")
    for file in filenames:
        table = file.split("/")[-1].rstrip(".csv")
        f= open(file,"r")
        columns = f.readline().replace(",",", ")
        print(columns)
        query = """COPY {0}({1})
        FROM '{2}'
        DELIMITER ','
        CSV HEADER;""".format(table,columns,file)
        print(query)
        db_cursor.execute(query)
        db_conn.commit()

if __name__ == "__main__":
    pdf, vcf = sys.argv[1], sys.argv[2]
    csv = pdfparser(pdf)
    vcffilter(vcf,csv)
    input("Use Usagi or some other means of mapping, press ENTER when ready to select output...")
    print("Expected output: Comma seperated file(s) named after corresponding DB table (e.g. person.csv), containing columns of table as header and rows with OMOP-concept-mapped data.")
    postgres()
    db_cursor.close()
    db_conn.close()