
import pyspark
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext 
from Bio import Align
from Bio.Seq import Seq
import Bio.SeqIO as SeqIO
from Bio.Alphabet import generic_dna, generic_protein
from Bio import Align
from Bio import pairwise2

conf = SparkConf().setMaster('spark://cf42d10be4cb:6066')
sc = SparkContext.getOrCreate()

def align_sequences_first(seq1, seq2, local=False):
    if local:
        aligner.mode = 'local'
    aligner = Align.PairwiseAligner()
    score = aligner.score(seq1, seq2)
    return score 

#this is going to the map function and returns score and seq id
def align_sequences(seq1, seq2):
    alignments = pairwise2.align.localxx(seq1, seq2)
    score = alignments[0][2]
    return score


GAP_PENALTY = -2
MISSMATCH_PENALTY = 0
MATCH_SCORE = 1

def needleman_wunsch(v, w):
    #print("here")
    n = len(v) + 1
    m = len(w) + 1
    
    s = [[0 for j in range(m)] for i in range(n)]
    backtrack = [[None for j in range(m)] for i in range(n)]
    
    for i in range(1, n):
        s[i][0] = s[i - 1][0] + GAP_PENALTY
        backtrack[i][0] = (i - 1, 0)
        
    for j in range(1, m):
        s[0][j] = s[0][j - 1] + GAP_PENALTY
        backtrack[0][j] = (0, j - 1)
        
    for i in range(1, n):
        for j in range(1, m):

            from_top = s[i - 1][j] + GAP_PENALTY
            from_left = s[i][j - 1] + GAP_PENALTY
            
            if v[i - 1] == w[j - 1]:
                from_diagonal = s[i - 1][j - 1] + MATCH_SCORE
            else:
                from_diagonal = s[i - 1][j - 1] + MISSMATCH_PENALTY
                
            s[i][j] = max(from_top, from_left, from_diagonal)
            
            # # Opciono, rekonstrukcija poravnanja
            # if s[i][j] == from_top:
            #     backtrack[i][j] = (i - 1, j)
            # elif s[i][j] == from_left:
            #     backtrack[i][j] = (i, j - 1)
            # else:
            #     backtrack[i][j] = (i - 1, j - 1)
                
    i = n - 1
    j = m - 1
    
    # v_align = ''
    # w_align = ''
    
    # # Opciono, rekonstrukcija poravnanja
    # while backtrack[i][j] != None:
    #     if backtrack[i][j] == (i - 1, j):
    #         v_align = v[i - 1] + v_align 
    #         w_align = '-' + w_align
    #     elif backtrack[i][j] == (i, j - 1):
    #         v_align = '-' + v_align 
    #         w_align = w[j - 1] + w_align
    #     else:
    #         v_align = v[i - 1] + v_align
    #         w_align = w[j - 1] + w_align
            
    #     (i, j) = backtrack[i][j]
    
    score = s[n - 1][m - 1]
    return score


#score, v_align, w_align = needleman_wunsch(v, w)


seq = "MIFTAPAYAPALPSPLPLQQTIGDFCLAKRRERQGASDSLFIEAAIDRKWTIDDIEARLGRMAAALSSAW \
NITPGQKWHKTVAILASNCVDTLILSWAVHRIGGGCLMLQPTSSADEMAAHMDRVPPFAMFLSQDLLTLG \
QEAFQKSSLSSNVPFYSFSGAEAPKPQQTAVPVASQDAPNISTLDDLMATGKELPLLEKTSFSEGEASRR \
VAYYCTTSGTSGFQRVVAITHENIIASILQAGLFIEATKGPASEVTLAFLPFNHIYGLLITHTFTWRGDS \
TVVHSGFNMMEILISIGKHRINTLYLVPPIINALSRNASILDRFDMSSVRYIVNGGGPLPKEAFLKMKAA \
RPDWQIIPGWGQTEGCGIGSLSSPKDIFPGSSGVLLPGVRIRLRDDDGRIVQGLEEMGEIEIESPSGLFG \
YVDYADEALLSPPKEEEFWWPTGDVGLFRISPNGEQHLFIVDRIRDMIKVKGNQVAPGQIEDHLTKHAAV \
AETAVIGIADEVAGERALAFIVRESSHAREMSEADLRKIIQEHNDLELPEVCRLQDRIIFVDELPKSASG \
KILKRELRKQVATWSPPQK"
#ten file is shortened records file with : sed -e "10q" records_file.csv > ten_file.csv 
data = sc.textFile('fiftythousand_file.csv') \
    .map(lambda line: line.split(",")) \
    .mapValues(lambda x: align_sequences(seq, x)) \
    .takeOrdered(5, key = lambda x: -x[1]) 
print(data)
# sqlcontext = SQLContext(sc)

# df = sqlcontext.read.format('com.databricks.spark.csv').options(header='false').load( 'records_file.csv' )



# 
# res = df.rdd \
#     .mapValues(lambda x: (align_sequences(seq, x))) \
#     .reduceByKey() \
#     .collect() \
#     .sortBy(lambda x: x[1], ascending=False) \
#     .take(5)
# print(res)

# # .mapValues(lambda x: (align_sequences(seq, x))) \
# #   .sortBy(lambda x: x[0], ascending = False) \