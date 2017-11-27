#!/usr/bin/env bash

pdflatex finalreport.tex
bibtex finalreport.aux
pdflatex finalreport.tex
pdflatex finalreport.tex
open finalreport.pdf

