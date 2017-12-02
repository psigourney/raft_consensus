#!/usr/bin/env bash

pdflatex finalreport2.tex
bibtex finalreport2.aux
pdflatex finalreport2.tex
pdflatex finalreport2.tex
open finalreport2.pdf

