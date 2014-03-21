CODE_DIR="/cs/research/fmedia/data5/wmayor/github/code/"
HERITRIX_DIR="/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/"
HERITRIX_PASSWORD="8700mm0n9wnxkjc1cvtbdhdqyxlji7z0"
CRAWL_DIR="/cs/research/fmedia/data5/wmayor/github/crawl/"
WARCS_DIR="/cs/research/fmedia/data5/wmayor/github/warcs/"

CODE_DIR="/Users/william/Projects/warc-compression/experiments/"
HERITRIX_DIR="/usr/local/heritrix-3.1.1/"
CRAWL_DIR="/Users/william/Desktop/test_data/crawl/"
WARCS_DIR="/Users/william/Desktop/test_data/warcs/"

. "${CODE_DIR}venv/bin/activate"
export JAVA_OPTS=" -Xmx1024m"
kill -9 $(cat "${HERITRIX_DIR}heritrix.pid")
"${HERITRIX_DIR}bin/heritrix" --web-admin admin:"${HERITRIX_PASSWORD}" --web-bind-hosts 0.0.0.0

while [ ! -f "${CODE_DIR}/github/archive/process_stop" ]
do
    while [[ $( jobs | wc -l ) -ge 10 ]]
    do
        if [ -f "${CODE_DIR}/github/archive/process_stop" ]
        then
            echo "Stopping, waiting for archivers: $(date)"
            wait
            exit
        fi
        echo "No empty processes: $(date)"
        sleep 300
    done
    echo "Spawning new archiver"
    python -u "${CODE_DIR}github/archive/archive.py" "${CRAWL_DIR}" "${WARCS_DIR}" "${HERITRIX_PASSWORD}" &
done