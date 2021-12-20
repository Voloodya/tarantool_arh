.PHONY: html singlehtml pdf docx docx-only

version:=scm-1
lang:=en

html:
	sphinx-build \
		-A version="${version}" \
		-Dlanguage="${lang}" \
		-b dirhtml \
		-c "." \
		-W \
		-d "output/doctrees/html/${lang}/" \
		"source/" \
		"output/html/${lang}"

html-ru:
	$(MAKE) lang=ru html

autobuild:
	sphinx-autobuild -b dirhtml \
    --host 0.0.0.0 \
    --port 8001 \
	-Dlanguage="${lang}" \
    -c "." \
	-d "output/doctrees/html/${lang}/" \
    "/doc/source" \
    "/doc/output/html/${lang}/"

autobuild-ru:
	$(MAKE) lang=ru autobuild


singlehtml:
	sphinx-build \
		-A version="${version}" \
		-b singlehtml \
		-Dlanguage="${lang}" \
		-c "." \
		-W \
		-d "output/doctrees/singlehtml/${lang}/" \
		"source/" \
		"output/singlehtml/${lang}/"

singlehtml-ru:
	$(MAKE) lang=ru singlehtml

docx-only:
	pwd && ls -hal pandoc && \
	mkdir -p "output/docx/${lang}/" && \
	pandoc -s output/singlehtml/${lang}/index.html \
		-f html \
		-t docx \
		-o output/docx/${lang}/tdg.docx \
		-M lang=${lang} \
		-M toc-title=Оглавление \
		--filter ./pandoc/filter.py \
		--table-of-contents \
		--toc-depth=3 \
		--reference-doc pandoc/reference.docx

docx: singlehtml docx-only

docx-only-gost:
	mkdir -p output/docx && \
	cd output/singlehtml && \
	pandoc -s index-docx.html \
		-f html \
		-t docx \
		-o ../docx/tdg-gost.docx \
		-M lang=ru \
		-M toc-title=Оглавление \
		--filter ../../pandoc/filter.py \
		--table-of-contents \
		--toc-depth=3 \
		--reference-doc ../../pandoc/reference-gost.docx

docx-only-espd:
	mkdir -p output/docx && \
	cd output/singlehtml && \
	pandoc -s index-docx.html \
		-f html \
		-t docx \
		-o ../docx/tdg-espd.docx \
		-M lang=ru \
		-M toc-title=Оглавление \
		--filter ../../pandoc/filter-espd.py \
		--table-of-contents \
		--toc-depth=3 \
		--reference-doc ../../pandoc/reference-espd.docx

docx-gost: singlehtml docx-only-gost
docx-espd: singlehtml docx-only-espd

pdf:
	sphinx-build \
		-A version="${version}" \
		-b latex \
		-Dmaster_doc="index" \
		-Dlanguage="${lang}" \
		-c "." \
		-W \
		-d "output/doctrees/pdf/${lang}" \
		"source/" \
		"output/pdf/${lang}"
	cd output/pdf/${lang} && pdflatex -interaction nonstopmode tdg.tex && pdflatex -interaction nonstopmode tdg.tex

pdf-ru:
	$(MAKE) lang=ru pdf


json:
	sphinx-build -n -b json \
		-d "output/doctrees/json/${lang}" \
		-Dlanguage="${lang}" \
		-c "." \
		-W \
		"source/" \
		"output/json/_build_${lang}/json"

json-ru:
	$(MAKE) lang=ru json

gettext:
	sphinx-build -n -b gettext \
		-W \
		-c "." \
		"source/" \
		"source/locale/en"

cleanup-po:
	cd ./source/locale && python3 cleanup.py po