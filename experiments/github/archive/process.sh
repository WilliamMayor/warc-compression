CODE_DIR="/cs/research/fmedia/data5/wmayor/github/code/"
HERITRIX_DIR="/cs/research/fmedia/data5/wmayor/github/heritrix-3.1.1/"
HERITRIX_PASSWORD="8700mm0n9wnxkjc1cvtbdhdqyxlji7z0"
CRAWL_DIR="/cs/research/fmedia/data5/wmayor/github/crawl/"

CODE_DIR="/Users/william/Projects/warc-compression/experiments/github/"
HERITRIX_DIR="/usr/local/heritrix-3.1.1/"
HERITRIX_PASSWORD="8700mm0n9wnxkjc1cvtbdhdqyxlji7z0"
CRAWL_DIR="/Users/william/Desktop/test_data/crawl/"

. "${CODE_DIR}venv/bin/activate"
export JAVA_OPTS=" -Xmx1024m"
kill -9 $(cat "${HERITRIX_DIR}heritrix.pid")
"${HERITRIX_DIR}bin/heritrix" --web-admin admin:"${HERITRIX_PASSWORD}" --web-bind-hosts 0.0.0.0

while [ ! -f "${CODE_DIR}process_stop" ]
do
    while [[ $( jobs | wc -l ) -ge 10 ]]
    do
        if [ -f "${CODE_DIR}process_stop" ]
        then
            echo "Stopping, waiting for archivers: $(date)"
            wait
            exit
        fi
        echo "No empty processes: $(date)"
        sleep 300
    done
    echo "Spawning new archiver"
    python -u "${CODE_DIR}archive/archive.py" "${CRAWL_DIR}" "${HERITRIX_PASSWORD}" &
done