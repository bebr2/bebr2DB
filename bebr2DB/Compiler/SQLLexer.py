# Generated from D:\bebr2DB\bebr2DB\Compiler\SQL.g4 by ANTLR 4.11.1
from antlr4 import *
from io import StringIO
import sys
if sys.version_info[1] > 5:
    from typing import TextIO
else:
    from typing.io import TextIO


def serializedATN():
    return [
        4,0,69,514,6,-1,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,
        2,6,7,6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,
        13,7,13,2,14,7,14,2,15,7,15,2,16,7,16,2,17,7,17,2,18,7,18,2,19,7,
        19,2,20,7,20,2,21,7,21,2,22,7,22,2,23,7,23,2,24,7,24,2,25,7,25,2,
        26,7,26,2,27,7,27,2,28,7,28,2,29,7,29,2,30,7,30,2,31,7,31,2,32,7,
        32,2,33,7,33,2,34,7,34,2,35,7,35,2,36,7,36,2,37,7,37,2,38,7,38,2,
        39,7,39,2,40,7,40,2,41,7,41,2,42,7,42,2,43,7,43,2,44,7,44,2,45,7,
        45,2,46,7,46,2,47,7,47,2,48,7,48,2,49,7,49,2,50,7,50,2,51,7,51,2,
        52,7,52,2,53,7,53,2,54,7,54,2,55,7,55,2,56,7,56,2,57,7,57,2,58,7,
        58,2,59,7,59,2,60,7,60,2,61,7,61,2,62,7,62,2,63,7,63,2,64,7,64,2,
        65,7,65,2,66,7,66,2,67,7,67,2,68,7,68,1,0,1,0,1,1,1,1,1,1,1,1,1,
        1,1,1,1,1,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,3,1,3,1,3,1,3,1,
        3,1,4,1,4,1,4,1,4,1,4,1,5,1,5,1,5,1,5,1,5,1,5,1,5,1,5,1,5,1,5,1,
        6,1,6,1,6,1,6,1,7,1,7,1,7,1,7,1,7,1,7,1,7,1,8,1,8,1,8,1,8,1,8,1,
        8,1,8,1,8,1,9,1,9,1,9,1,9,1,9,1,10,1,10,1,10,1,10,1,10,1,11,1,11,
        1,11,1,11,1,11,1,12,1,12,1,12,1,13,1,13,1,13,1,13,1,13,1,13,1,14,
        1,14,1,14,1,14,1,14,1,15,1,15,1,16,1,16,1,17,1,17,1,17,1,17,1,17,
        1,18,1,18,1,18,1,18,1,18,1,18,1,18,1,19,1,19,1,19,1,19,1,19,1,20,
        1,20,1,20,1,20,1,20,1,20,1,20,1,21,1,21,1,21,1,21,1,21,1,21,1,21,
        1,22,1,22,1,22,1,22,1,22,1,22,1,23,1,23,1,23,1,23,1,23,1,23,1,23,
        1,24,1,24,1,24,1,24,1,25,1,25,1,25,1,25,1,25,1,25,1,25,1,26,1,26,
        1,26,1,26,1,26,1,26,1,27,1,27,1,27,1,28,1,28,1,28,1,28,1,28,1,28,
        1,29,1,29,1,29,1,29,1,29,1,29,1,29,1,30,1,30,1,30,1,30,1,30,1,30,
        1,31,1,31,1,31,1,31,1,32,1,32,1,32,1,32,1,32,1,32,1,33,1,33,1,33,
        1,33,1,33,1,33,1,33,1,33,1,34,1,34,1,34,1,34,1,35,1,35,1,35,1,35,
        1,35,1,35,1,35,1,35,1,36,1,36,1,36,1,36,1,36,1,36,1,36,1,36,1,36,
        1,36,1,36,1,37,1,37,1,37,1,37,1,37,1,37,1,37,1,37,1,37,1,37,1,37,
        1,38,1,38,1,38,1,38,1,38,1,38,1,38,1,39,1,39,1,40,1,40,1,40,1,40,
        1,41,1,41,1,41,1,41,1,41,1,41,1,41,1,41,1,42,1,42,1,42,1,42,1,43,
        1,43,1,43,1,43,1,43,1,43,1,43,1,43,1,44,1,44,1,44,1,44,1,44,1,44,
        1,45,1,45,1,45,1,45,1,46,1,46,1,46,1,47,1,47,1,47,1,48,1,48,1,48,
        1,48,1,48,1,49,1,49,1,50,1,50,1,51,1,51,1,52,1,52,1,53,1,53,1,53,
        1,54,1,54,1,55,1,55,1,55,1,56,1,56,1,56,1,57,1,57,1,57,1,57,1,57,
        1,57,1,58,1,58,1,58,1,58,1,59,1,59,1,59,1,59,1,60,1,60,1,60,1,60,
        1,61,1,61,1,61,1,61,1,62,1,62,1,62,1,62,1,62,1,63,1,63,5,63,467,
        8,63,10,63,12,63,470,9,63,1,64,4,64,473,8,64,11,64,12,64,474,1,65,
        1,65,5,65,479,8,65,10,65,12,65,482,9,65,1,65,1,65,1,66,3,66,487,
        8,66,1,66,4,66,490,8,66,11,66,12,66,491,1,66,1,66,5,66,496,8,66,
        10,66,12,66,499,9,66,1,67,4,67,502,8,67,11,67,12,67,503,1,67,1,67,
        1,68,1,68,1,68,4,68,511,8,68,11,68,12,68,512,0,0,69,1,1,3,2,5,3,
        7,4,9,5,11,6,13,7,15,8,17,9,19,10,21,11,23,12,25,13,27,14,29,15,
        31,16,33,17,35,18,37,19,39,20,41,21,43,22,45,23,47,24,49,25,51,26,
        53,27,55,28,57,29,59,30,61,31,63,32,65,33,67,34,69,35,71,36,73,37,
        75,38,77,39,79,40,81,41,83,42,85,43,87,44,89,45,91,46,93,47,95,48,
        97,49,99,50,101,51,103,52,105,53,107,54,109,55,111,56,113,57,115,
        58,117,59,119,60,121,61,123,62,125,63,127,64,129,65,131,66,133,67,
        135,68,137,69,1,0,6,3,0,65,90,95,95,97,122,4,0,48,57,65,90,95,95,
        97,122,1,0,48,57,1,0,39,39,3,0,9,10,13,13,32,32,1,0,59,59,521,0,
        1,1,0,0,0,0,3,1,0,0,0,0,5,1,0,0,0,0,7,1,0,0,0,0,9,1,0,0,0,0,11,1,
        0,0,0,0,13,1,0,0,0,0,15,1,0,0,0,0,17,1,0,0,0,0,19,1,0,0,0,0,21,1,
        0,0,0,0,23,1,0,0,0,0,25,1,0,0,0,0,27,1,0,0,0,0,29,1,0,0,0,0,31,1,
        0,0,0,0,33,1,0,0,0,0,35,1,0,0,0,0,37,1,0,0,0,0,39,1,0,0,0,0,41,1,
        0,0,0,0,43,1,0,0,0,0,45,1,0,0,0,0,47,1,0,0,0,0,49,1,0,0,0,0,51,1,
        0,0,0,0,53,1,0,0,0,0,55,1,0,0,0,0,57,1,0,0,0,0,59,1,0,0,0,0,61,1,
        0,0,0,0,63,1,0,0,0,0,65,1,0,0,0,0,67,1,0,0,0,0,69,1,0,0,0,0,71,1,
        0,0,0,0,73,1,0,0,0,0,75,1,0,0,0,0,77,1,0,0,0,0,79,1,0,0,0,0,81,1,
        0,0,0,0,83,1,0,0,0,0,85,1,0,0,0,0,87,1,0,0,0,0,89,1,0,0,0,0,91,1,
        0,0,0,0,93,1,0,0,0,0,95,1,0,0,0,0,97,1,0,0,0,0,99,1,0,0,0,0,101,
        1,0,0,0,0,103,1,0,0,0,0,105,1,0,0,0,0,107,1,0,0,0,0,109,1,0,0,0,
        0,111,1,0,0,0,0,113,1,0,0,0,0,115,1,0,0,0,0,117,1,0,0,0,0,119,1,
        0,0,0,0,121,1,0,0,0,0,123,1,0,0,0,0,125,1,0,0,0,0,127,1,0,0,0,0,
        129,1,0,0,0,0,131,1,0,0,0,0,133,1,0,0,0,0,135,1,0,0,0,0,137,1,0,
        0,0,1,139,1,0,0,0,3,141,1,0,0,0,5,148,1,0,0,0,7,157,1,0,0,0,9,162,
        1,0,0,0,11,167,1,0,0,0,13,177,1,0,0,0,15,181,1,0,0,0,17,188,1,0,
        0,0,19,196,1,0,0,0,21,201,1,0,0,0,23,206,1,0,0,0,25,211,1,0,0,0,
        27,214,1,0,0,0,29,220,1,0,0,0,31,225,1,0,0,0,33,227,1,0,0,0,35,229,
        1,0,0,0,37,234,1,0,0,0,39,241,1,0,0,0,41,246,1,0,0,0,43,253,1,0,
        0,0,45,260,1,0,0,0,47,266,1,0,0,0,49,273,1,0,0,0,51,277,1,0,0,0,
        53,284,1,0,0,0,55,290,1,0,0,0,57,293,1,0,0,0,59,299,1,0,0,0,61,306,
        1,0,0,0,63,312,1,0,0,0,65,316,1,0,0,0,67,322,1,0,0,0,69,330,1,0,
        0,0,71,334,1,0,0,0,73,342,1,0,0,0,75,353,1,0,0,0,77,364,1,0,0,0,
        79,371,1,0,0,0,81,373,1,0,0,0,83,377,1,0,0,0,85,385,1,0,0,0,87,389,
        1,0,0,0,89,397,1,0,0,0,91,403,1,0,0,0,93,407,1,0,0,0,95,410,1,0,
        0,0,97,413,1,0,0,0,99,418,1,0,0,0,101,420,1,0,0,0,103,422,1,0,0,
        0,105,424,1,0,0,0,107,426,1,0,0,0,109,429,1,0,0,0,111,431,1,0,0,
        0,113,434,1,0,0,0,115,437,1,0,0,0,117,443,1,0,0,0,119,447,1,0,0,
        0,121,451,1,0,0,0,123,455,1,0,0,0,125,459,1,0,0,0,127,464,1,0,0,
        0,129,472,1,0,0,0,131,476,1,0,0,0,133,486,1,0,0,0,135,501,1,0,0,
        0,137,507,1,0,0,0,139,140,5,59,0,0,140,2,1,0,0,0,141,142,5,67,0,
        0,142,143,5,82,0,0,143,144,5,69,0,0,144,145,5,65,0,0,145,146,5,84,
        0,0,146,147,5,69,0,0,147,4,1,0,0,0,148,149,5,68,0,0,149,150,5,65,
        0,0,150,151,5,84,0,0,151,152,5,65,0,0,152,153,5,66,0,0,153,154,5,
        65,0,0,154,155,5,83,0,0,155,156,5,69,0,0,156,6,1,0,0,0,157,158,5,
        68,0,0,158,159,5,82,0,0,159,160,5,79,0,0,160,161,5,80,0,0,161,8,
        1,0,0,0,162,163,5,83,0,0,163,164,5,72,0,0,164,165,5,79,0,0,165,166,
        5,87,0,0,166,10,1,0,0,0,167,168,5,68,0,0,168,169,5,65,0,0,169,170,
        5,84,0,0,170,171,5,65,0,0,171,172,5,66,0,0,172,173,5,65,0,0,173,
        174,5,83,0,0,174,175,5,69,0,0,175,176,5,83,0,0,176,12,1,0,0,0,177,
        178,5,85,0,0,178,179,5,83,0,0,179,180,5,69,0,0,180,14,1,0,0,0,181,
        182,5,84,0,0,182,183,5,65,0,0,183,184,5,66,0,0,184,185,5,76,0,0,
        185,186,5,69,0,0,186,187,5,83,0,0,187,16,1,0,0,0,188,189,5,73,0,
        0,189,190,5,78,0,0,190,191,5,68,0,0,191,192,5,69,0,0,192,193,5,88,
        0,0,193,194,5,69,0,0,194,195,5,83,0,0,195,18,1,0,0,0,196,197,5,76,
        0,0,197,198,5,79,0,0,198,199,5,65,0,0,199,200,5,68,0,0,200,20,1,
        0,0,0,201,202,5,70,0,0,202,203,5,82,0,0,203,204,5,79,0,0,204,205,
        5,77,0,0,205,22,1,0,0,0,206,207,5,70,0,0,207,208,5,73,0,0,208,209,
        5,76,0,0,209,210,5,69,0,0,210,24,1,0,0,0,211,212,5,84,0,0,212,213,
        5,79,0,0,213,26,1,0,0,0,214,215,5,84,0,0,215,216,5,65,0,0,216,217,
        5,66,0,0,217,218,5,76,0,0,218,219,5,69,0,0,219,28,1,0,0,0,220,221,
        5,68,0,0,221,222,5,85,0,0,222,223,5,77,0,0,223,224,5,80,0,0,224,
        30,1,0,0,0,225,226,5,40,0,0,226,32,1,0,0,0,227,228,5,41,0,0,228,
        34,1,0,0,0,229,230,5,68,0,0,230,231,5,69,0,0,231,232,5,83,0,0,232,
        233,5,67,0,0,233,36,1,0,0,0,234,235,5,73,0,0,235,236,5,78,0,0,236,
        237,5,83,0,0,237,238,5,69,0,0,238,239,5,82,0,0,239,240,5,84,0,0,
        240,38,1,0,0,0,241,242,5,73,0,0,242,243,5,78,0,0,243,244,5,84,0,
        0,244,245,5,79,0,0,245,40,1,0,0,0,246,247,5,86,0,0,247,248,5,65,
        0,0,248,249,5,76,0,0,249,250,5,85,0,0,250,251,5,69,0,0,251,252,5,
        83,0,0,252,42,1,0,0,0,253,254,5,68,0,0,254,255,5,69,0,0,255,256,
        5,76,0,0,256,257,5,69,0,0,257,258,5,84,0,0,258,259,5,69,0,0,259,
        44,1,0,0,0,260,261,5,87,0,0,261,262,5,72,0,0,262,263,5,69,0,0,263,
        264,5,82,0,0,264,265,5,69,0,0,265,46,1,0,0,0,266,267,5,85,0,0,267,
        268,5,80,0,0,268,269,5,68,0,0,269,270,5,65,0,0,270,271,5,84,0,0,
        271,272,5,69,0,0,272,48,1,0,0,0,273,274,5,83,0,0,274,275,5,69,0,
        0,275,276,5,84,0,0,276,50,1,0,0,0,277,278,5,83,0,0,278,279,5,69,
        0,0,279,280,5,76,0,0,280,281,5,69,0,0,281,282,5,67,0,0,282,283,5,
        84,0,0,283,52,1,0,0,0,284,285,5,71,0,0,285,286,5,82,0,0,286,287,
        5,79,0,0,287,288,5,85,0,0,288,289,5,80,0,0,289,54,1,0,0,0,290,291,
        5,66,0,0,291,292,5,89,0,0,292,56,1,0,0,0,293,294,5,76,0,0,294,295,
        5,73,0,0,295,296,5,77,0,0,296,297,5,73,0,0,297,298,5,84,0,0,298,
        58,1,0,0,0,299,300,5,79,0,0,300,301,5,70,0,0,301,302,5,70,0,0,302,
        303,5,83,0,0,303,304,5,69,0,0,304,305,5,84,0,0,305,60,1,0,0,0,306,
        307,5,65,0,0,307,308,5,76,0,0,308,309,5,84,0,0,309,310,5,69,0,0,
        310,311,5,82,0,0,311,62,1,0,0,0,312,313,5,65,0,0,313,314,5,68,0,
        0,314,315,5,68,0,0,315,64,1,0,0,0,316,317,5,73,0,0,317,318,5,78,
        0,0,318,319,5,68,0,0,319,320,5,69,0,0,320,321,5,88,0,0,321,66,1,
        0,0,0,322,323,5,80,0,0,323,324,5,82,0,0,324,325,5,73,0,0,325,326,
        5,77,0,0,326,327,5,65,0,0,327,328,5,82,0,0,328,329,5,89,0,0,329,
        68,1,0,0,0,330,331,5,75,0,0,331,332,5,69,0,0,332,333,5,89,0,0,333,
        70,1,0,0,0,334,335,5,70,0,0,335,336,5,79,0,0,336,337,5,82,0,0,337,
        338,5,69,0,0,338,339,5,73,0,0,339,340,5,71,0,0,340,341,5,78,0,0,
        341,72,1,0,0,0,342,343,5,67,0,0,343,344,5,79,0,0,344,345,5,78,0,
        0,345,346,5,83,0,0,346,347,5,84,0,0,347,348,5,82,0,0,348,349,5,65,
        0,0,349,350,5,73,0,0,350,351,5,78,0,0,351,352,5,84,0,0,352,74,1,
        0,0,0,353,354,5,82,0,0,354,355,5,69,0,0,355,356,5,70,0,0,356,357,
        5,69,0,0,357,358,5,82,0,0,358,359,5,69,0,0,359,360,5,78,0,0,360,
        361,5,67,0,0,361,362,5,69,0,0,362,363,5,83,0,0,363,76,1,0,0,0,364,
        365,5,85,0,0,365,366,5,78,0,0,366,367,5,73,0,0,367,368,5,81,0,0,
        368,369,5,85,0,0,369,370,5,69,0,0,370,78,1,0,0,0,371,372,5,44,0,
        0,372,80,1,0,0,0,373,374,5,78,0,0,374,375,5,79,0,0,375,376,5,84,
        0,0,376,82,1,0,0,0,377,378,5,68,0,0,378,379,5,69,0,0,379,380,5,70,
        0,0,380,381,5,65,0,0,381,382,5,85,0,0,382,383,5,76,0,0,383,384,5,
        84,0,0,384,84,1,0,0,0,385,386,5,73,0,0,386,387,5,78,0,0,387,388,
        5,84,0,0,388,86,1,0,0,0,389,390,5,86,0,0,390,391,5,65,0,0,391,392,
        5,82,0,0,392,393,5,67,0,0,393,394,5,72,0,0,394,395,5,65,0,0,395,
        396,5,82,0,0,396,88,1,0,0,0,397,398,5,70,0,0,398,399,5,76,0,0,399,
        400,5,79,0,0,400,401,5,65,0,0,401,402,5,84,0,0,402,90,1,0,0,0,403,
        404,5,65,0,0,404,405,5,78,0,0,405,406,5,68,0,0,406,92,1,0,0,0,407,
        408,5,73,0,0,408,409,5,83,0,0,409,94,1,0,0,0,410,411,5,73,0,0,411,
        412,5,78,0,0,412,96,1,0,0,0,413,414,5,76,0,0,414,415,5,73,0,0,415,
        416,5,75,0,0,416,417,5,69,0,0,417,98,1,0,0,0,418,419,5,46,0,0,419,
        100,1,0,0,0,420,421,5,42,0,0,421,102,1,0,0,0,422,423,5,61,0,0,423,
        104,1,0,0,0,424,425,5,60,0,0,425,106,1,0,0,0,426,427,5,60,0,0,427,
        428,5,61,0,0,428,108,1,0,0,0,429,430,5,62,0,0,430,110,1,0,0,0,431,
        432,5,62,0,0,432,433,5,61,0,0,433,112,1,0,0,0,434,435,5,60,0,0,435,
        436,5,62,0,0,436,114,1,0,0,0,437,438,5,67,0,0,438,439,5,79,0,0,439,
        440,5,85,0,0,440,441,5,78,0,0,441,442,5,84,0,0,442,116,1,0,0,0,443,
        444,5,65,0,0,444,445,5,86,0,0,445,446,5,71,0,0,446,118,1,0,0,0,447,
        448,5,77,0,0,448,449,5,65,0,0,449,450,5,88,0,0,450,120,1,0,0,0,451,
        452,5,77,0,0,452,453,5,73,0,0,453,454,5,78,0,0,454,122,1,0,0,0,455,
        456,5,83,0,0,456,457,5,85,0,0,457,458,5,77,0,0,458,124,1,0,0,0,459,
        460,5,78,0,0,460,461,5,85,0,0,461,462,5,76,0,0,462,463,5,76,0,0,
        463,126,1,0,0,0,464,468,7,0,0,0,465,467,7,1,0,0,466,465,1,0,0,0,
        467,470,1,0,0,0,468,466,1,0,0,0,468,469,1,0,0,0,469,128,1,0,0,0,
        470,468,1,0,0,0,471,473,7,2,0,0,472,471,1,0,0,0,473,474,1,0,0,0,
        474,472,1,0,0,0,474,475,1,0,0,0,475,130,1,0,0,0,476,480,5,39,0,0,
        477,479,8,3,0,0,478,477,1,0,0,0,479,482,1,0,0,0,480,478,1,0,0,0,
        480,481,1,0,0,0,481,483,1,0,0,0,482,480,1,0,0,0,483,484,5,39,0,0,
        484,132,1,0,0,0,485,487,5,45,0,0,486,485,1,0,0,0,486,487,1,0,0,0,
        487,489,1,0,0,0,488,490,7,2,0,0,489,488,1,0,0,0,490,491,1,0,0,0,
        491,489,1,0,0,0,491,492,1,0,0,0,492,493,1,0,0,0,493,497,5,46,0,0,
        494,496,7,2,0,0,495,494,1,0,0,0,496,499,1,0,0,0,497,495,1,0,0,0,
        497,498,1,0,0,0,498,134,1,0,0,0,499,497,1,0,0,0,500,502,7,4,0,0,
        501,500,1,0,0,0,502,503,1,0,0,0,503,501,1,0,0,0,503,504,1,0,0,0,
        504,505,1,0,0,0,505,506,6,67,0,0,506,136,1,0,0,0,507,508,5,45,0,
        0,508,510,5,45,0,0,509,511,8,5,0,0,510,509,1,0,0,0,511,512,1,0,0,
        0,512,510,1,0,0,0,512,513,1,0,0,0,513,138,1,0,0,0,9,0,468,474,480,
        486,491,497,503,512,1,6,0,0
    ]

class SQLLexer(Lexer):

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    T__0 = 1
    T__1 = 2
    T__2 = 3
    T__3 = 4
    T__4 = 5
    T__5 = 6
    T__6 = 7
    T__7 = 8
    T__8 = 9
    T__9 = 10
    T__10 = 11
    T__11 = 12
    T__12 = 13
    T__13 = 14
    T__14 = 15
    T__15 = 16
    T__16 = 17
    T__17 = 18
    T__18 = 19
    T__19 = 20
    T__20 = 21
    T__21 = 22
    T__22 = 23
    T__23 = 24
    T__24 = 25
    T__25 = 26
    T__26 = 27
    T__27 = 28
    T__28 = 29
    T__29 = 30
    T__30 = 31
    T__31 = 32
    T__32 = 33
    T__33 = 34
    T__34 = 35
    T__35 = 36
    T__36 = 37
    T__37 = 38
    T__38 = 39
    T__39 = 40
    T__40 = 41
    T__41 = 42
    T__42 = 43
    T__43 = 44
    T__44 = 45
    T__45 = 46
    T__46 = 47
    T__47 = 48
    T__48 = 49
    T__49 = 50
    T__50 = 51
    EqualOrAssign = 52
    Less = 53
    LessEqual = 54
    Greater = 55
    GreaterEqual = 56
    NotEqual = 57
    Count = 58
    Average = 59
    Max = 60
    Min = 61
    Sum = 62
    Null = 63
    Identifier = 64
    Integer = 65
    String = 66
    Float = 67
    Whitespace = 68
    Annotation = 69

    channelNames = [ u"DEFAULT_TOKEN_CHANNEL", u"HIDDEN" ]

    modeNames = [ "DEFAULT_MODE" ]

    literalNames = [ "<INVALID>",
            "';'", "'CREATE'", "'DATABASE'", "'DROP'", "'SHOW'", "'DATABASES'", 
            "'USE'", "'TABLES'", "'INDEXES'", "'LOAD'", "'FROM'", "'FILE'", 
            "'TO'", "'TABLE'", "'DUMP'", "'('", "')'", "'DESC'", "'INSERT'", 
            "'INTO'", "'VALUES'", "'DELETE'", "'WHERE'", "'UPDATE'", "'SET'", 
            "'SELECT'", "'GROUP'", "'BY'", "'LIMIT'", "'OFFSET'", "'ALTER'", 
            "'ADD'", "'INDEX'", "'PRIMARY'", "'KEY'", "'FOREIGN'", "'CONSTRAINT'", 
            "'REFERENCES'", "'UNIQUE'", "','", "'NOT'", "'DEFAULT'", "'INT'", 
            "'VARCHAR'", "'FLOAT'", "'AND'", "'IS'", "'IN'", "'LIKE'", "'.'", 
            "'*'", "'='", "'<'", "'<='", "'>'", "'>='", "'<>'", "'COUNT'", 
            "'AVG'", "'MAX'", "'MIN'", "'SUM'", "'NULL'" ]

    symbolicNames = [ "<INVALID>",
            "EqualOrAssign", "Less", "LessEqual", "Greater", "GreaterEqual", 
            "NotEqual", "Count", "Average", "Max", "Min", "Sum", "Null", 
            "Identifier", "Integer", "String", "Float", "Whitespace", "Annotation" ]

    ruleNames = [ "T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", 
                  "T__7", "T__8", "T__9", "T__10", "T__11", "T__12", "T__13", 
                  "T__14", "T__15", "T__16", "T__17", "T__18", "T__19", 
                  "T__20", "T__21", "T__22", "T__23", "T__24", "T__25", 
                  "T__26", "T__27", "T__28", "T__29", "T__30", "T__31", 
                  "T__32", "T__33", "T__34", "T__35", "T__36", "T__37", 
                  "T__38", "T__39", "T__40", "T__41", "T__42", "T__43", 
                  "T__44", "T__45", "T__46", "T__47", "T__48", "T__49", 
                  "T__50", "EqualOrAssign", "Less", "LessEqual", "Greater", 
                  "GreaterEqual", "NotEqual", "Count", "Average", "Max", 
                  "Min", "Sum", "Null", "Identifier", "Integer", "String", 
                  "Float", "Whitespace", "Annotation" ]

    grammarFileName = "SQL.g4"

    def __init__(self, input=None, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.11.1")
        self._interp = LexerATNSimulator(self, self.atn, self.decisionsToDFA, PredictionContextCache())
        self._actions = None
        self._predicates = None


