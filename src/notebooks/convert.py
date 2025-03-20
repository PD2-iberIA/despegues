import json
import codecs
import argparse

argumentParser = argparse.ArgumentParser()
argumentParser.add_argument('--method', required=True, default='z2j', help='From Zeppelin To Jupyter: z2j, From Jupyter To Zeppelin: j2z')
argumentParser.add_argument('--file_path', required=True, help='Path of file to be converted.')
argumentParser.add_argument('--interpreter', help='Name of Zeppelin interpreter. (optional)')
args = vars(argumentParser.parse_args())


def load_data(file_path):
    '''
    Loads data given file path.

    :param file_path: path of file
    :return: data to be processed
    '''
    return json.load(codecs.open(file_path, 'r', 'utf-8-sig'))


def handle_z2j(data):
    '''
    Generates Jupyter .ipynb file from data obtained from Zeppelin file

    :param data: data loaded from given file path
    '''
    file_name = data['name']
    paragraphs = data['paragraphs']
    jupyter_data = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## {0}".format(file_name)
                ]
            },
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "",
                "language": "python",
                "name": ""
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.7.10"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    jupyter_cells = jupyter_data['cells']
    for paragraph in paragraphs:
        if 'text' in paragraph.keys():
            paragraph_text = paragraph['text']
            sentences = paragraph_text.split('\n')
            source = []
            for i in range(len(sentences)):
                sentence = sentences[i]
                if not '.ipython' in sentence and not sentence == '':
                    if not i == (len(sentences) - 1):
                        source.append(sentence + '\n')
                    else:
                        source.append(sentence)
            jupyter_cell = {
                "cell_type": "code",
                "execution_count": '',
                "metadata": {},
                "outputs": [],
                "source": source
            }
            jupyter_cells.append(jupyter_cell)
    jupyter_data['cells'] = jupyter_cells
    with open('converted.ipynb', 'w') as convert_file:
        convert_file.write(json.dumps(jupyter_data))

def handle_j2z(data):
    '''
    Generates Zeppelin .json file from data obtained from Jupyter file

    :param data: data loaded from given file path
    '''
    zeppelin_data = {
        "paragraphs": [],
        "name": "converted",
        "id": "",
        "noteParams": {},
        "noteForms": {},
        "angularObjects": {
            "md:shared_process": [],
            "anaconda:shared_process": [],
            "angular:shared_process": [],
            "jdbc:shared_process": []
        },
        "config": {
            "isZeppelinNotebookCronEnable": True,
            "looknfeel": "default",
            "personalizedMode": "false"
        },
        "info": {}
    }
    zeppelin_paragraphs = zeppelin_data['paragraphs']
    interpreter_name = 'livy3.pyspark'
    cells = data['cells']
    for cell in cells:
        cell_type = cell['cell_type']
        source = cell['source']
        text = '%{0}\n'.format(interpreter_name)
        for i in range(len(source)):
            sentence = source[i]
            if not i == (len(source) - 1):
                if cell_type == 'markdown':
                    text += '# ' + sentence + '\n'
                else:
                    text += sentence + '\n'
            else:
                if cell_type == 'markdown':
                    text += '# ' + sentence
                else:
                    text += sentence
        zeppelin_paragraph = {
            "text": text,
            "config": {
                "editorSetting": {
                    "language": "python",
                    "editOnDblClick": False,
                    "completionSupport": True,
                    "completionKey": "TAB"
                },
                "colWidth": 12,
                "editorMode": "ace/mode/python",
                "fontSize": 9,
                "results": {},
                "enabled": True
            },
            "settings": {
                "params": {},
                "forms": {}
            },
            "results": {
                "code": "SUCCESS",
                "msg": []
            },
            "apps": [],
            "status": "FINISHED",
            "progressUpdateIntervalMs": 500,
            "focus": True
        }
        zeppelin_paragraphs.append(zeppelin_paragraph)
    zeppelin_data['paragraphs'] = zeppelin_paragraphs
    with open('converted.json', 'w') as convert_file:
        convert_file.write(json.dumps(zeppelin_data))

if __name__ == '__main__':
    file_path = args['file_path']
    method = args['method']
    data = load_data(file_path)
    if method == 'z2j':
        handle_z2j(data)
    elif method == 'j2z':
        handle_j2z(data)
    else:
        print('Incorrect method {0}. Choose z2j or j2z.'.format(method))

