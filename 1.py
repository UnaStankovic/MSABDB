#This file will prepare the data for cassandra import

"""
The fasta format is:
swpr seq id  protein id      description
>sp|Q6GZX4|001R_FRG3G Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) OX=654924 GN=FV3-001R PE=4 SV=1
MAFSAEDVLKEYDRRRRMEALLLSLYYPNDRKLLDYKEWSPPRVQVECPKAPVEWNNPPS
EKGLIVGHFSGIKYKGEKAQASEVDVNKMCCWVSKFKDAMRRYQGIQTCKIPGKVLSDLD
AKIKAYNLTVEGVEGFVRYSRVTKQHVAAFLKELRHSKQYENVNLIHYILTDKRVDIQHL
EKDLVKDFKALVESAHRMRQGHMINVKYILYQLLKKHGHGPDGPDILTVKTGSKGVLYDD
SFRKIYTDLGWKFTPL

The adequate format may be the following:
db : sp <- Swiss Prot       
seq_id : Q6GZX4
protein_id : 001R_FRG3G
----> these three make one id 
id : sp|Q6GZX4|001R_FRG3G 
description : Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) OX=654924 GN=FV3-001R PE=4 SV=1 <---- unnecessary
sequence: MAFSAEDVLKEYDRRRRMEALLLSLYYPNDRKLLDYKEWSPPRVQVECPKAPVEWNNPPS
EKGLIVGHFSGIKYKGEKAQASEVDVNKMCCWVSKFKDAMRRYQGIQTCKIPGKVLSDLD
AKIKAYNLTVEGVEGFVRYSRVTKQHVAAFLKELRHSKQYENVNLIHYILTDKRVDIQHL
EKDLVKDFKALVESAHRMRQGHMINVKYILYQLLKKHGHGPDGPDILTVKTGSKGVLYDD
SFRKIYTDLGWKFTPL

In the end, just id and the sequence. Because for the sequence alignment we don't need all the extra info.

id : sp|Q6GZX4|001R_FRG3G ,
sequence : MAFSAEDVLKEYDRRRRMEALLLSLYYPNDRKLLDYKEWSPPRVQVECPKAPVEWNNPPS
EKGLIVGHFSGIKYKGEKAQASEVDVNKMCCWVSKFKDAMRRYQGIQTCKIPGKVLSDLD
AKIKAYNLTVEGVEGFVRYSRVTKQHVAAFLKELRHSKQYENVNLIHYILTDKRVDIQHL
EKDLVKDFKALVESAHRMRQGHMINVKYILYQLLKKHGHGPDGPDILTVKTGSKGVLYDD
SFRKIYTDLGWKFTPL

"""
from Bio.Seq import Seq
import Bio.SeqIO as SeqIO
from Bio.Alphabet import generic_dna, generic_protein

#If access to records by number is requested
records = list(SeqIO.parse("uniprot_sprot.fasta", "fasta"))
# print(len(records))
# print(records[1].id)
# print(records[1].seq)
print(records[1].name)

#writing to the csv file which will later be used to create database
import csv

with open('records_file.csv', mode='w') as records_file:
    records_writer = csv.writer(records_file, delimiter=',')
    for record in records:
        records_writer.writerow([record.id, record.seq])

records_file.close()
   
"""
#access by the key
record_dict = SeqIO.to_dict(SeqIO.parse("uniprot_sprot.fasta", "fasta"))
print(len(record_dict["sp|Q6GZX3|002L_FRG3G"]))

#indexing approach with dictionary-like access to any record - the best memory saver
#although it may be the best memory saver it requires more extraction, parsing and stuff 
record_dict = SeqIO.index("uniprot_sprot.fasta", "fasta")
for d in record_dict:
    print(record_dict.get_raw(d))

record_dict.close()

"""