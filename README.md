# AdvancedDB Project
## Σύντομη περιγραφή
<p align="justify">Το παρόν Project υλοποιήθηκε στο πλαίσιο του μάθηματος "Προχωρημένα Θέματα Βάσεων Δεδομένων" του 9ου εξαμήνου. Η εκφώνηση της εργασίας βρίσκεται <a href=https://helios.ntua.gr/pluginfile.php/175280/mod_resource/content/2/AdvancedDB_project_2022.pdf)>εδώ</a>.</p>

## Περιγραφή των αρχείων
<p align="justify">Ο κώδικας για τα queries υπάρχει σε 2 μορφές αρχείων, δηλαδή ως Python script <a href=https://github.com/alexandrosst/AdvancedDB_Project/blob/main/AdvancedDB_Project.py>AdvancedDB_Project.py</a> και ως Jupyter Notebook αρχείο <a href=https://github.com/alexandrosst/AdvancedDB_Project/blob/main/AdvancedDB_Project.ipynb>AdvancedDB_Project.ipynb</a>. Οι κώδικες των αρχείων αυτών είναι ισοδύναμοι από άποψη πληρότητας.<p>

## Πληροφορίες για μηχανήματα στον okeanos-knossos
- **Μηχάνημα 1 - master:**

|        Element       |     Value    |
|:--------------------:|:------------:|
|       CPU cores      |       2      |
|        Memory        |      4gb     |
|  public IPv4 address | 83.212.80.22 |
| private IPv4 address |  192.168.0.1 |

- **Μηχάνημα 2 - slave:**

|        Element       |     Value    |
|:--------------------:|:------------:|
|       CPU cores      |       2      |
|        Memory        |      4gb     |
| private IPv4 address |  192.168.0.2 |

## Εκτέλεση
<p align="justify">Αρχικά, υποθέτουμε ότι έχει γίνει η εγκατάσταση του hadoop 2.7, python 3.8 και του spark 3.1.3 όπως περιγράφεται στις οδηγίες του helios.</p>

<p align="justify">Στις εντολές παρακάτω έχει γίνει η υπόθεση ότι εργαζόμαστε στα μηχανήματα που μας παραχωρήθηκαν στον okeanos-knossos. Αν δουλεύουμε σε διαφορετικά μηχανήματα οφείλουμε να τροποποιήσουμε κατάλληλα τις διευθύνσεις IPv4 του master μηχανήματος στις εντολές για τους εργάτες, καθώς και στα αρχεία <a href=https://github.com/alexandrosst/AdvancedDB_Project/blob/main/AdvancedDB_Project.py>AdvancedDB_Project.py</a> και <a href=https://github.com/alexandrosst/AdvancedDB_Project/blob/main/AdvancedDB_Project.ipynb>AdvancedDB_Project.ipynb</a></p>

Έπειτα, χρειάζεται να ενεργοποιήσουμε τους εργάτες. Σε κάθε μηχάνημα εκτελούμε την εντολή:
```bash
spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 --webui-port 8080 --port 65509 --cores 2 --memory 4g spark://192.168.0.1:7077
```
Για να απενεργοποίησουμε κάποιον εργάτη, εκτελούμε στο αντίστοιχο μηχάνημα την εντολή:
```bash
spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1 --webui-port 8080 --port 65509 --cores 2 --memory 4g spark://192.168.0.1:7077
```

Έπειτα, μπορούμε να προχωρήσουμε στον κώδικα για τα queries. Έχουμε δύο περιπτώσεις:
- [AdvancedDB_Project.py](https://github.com/alexandrosst/AdvancedDB_Project/blob/main/AdvancedDB_Project.py)

    Μπορεί να εκτελεστεί με τη βοήθεια της εντολής:
    ```bash
    python3.8 AdvancedDB_Project.py
    ```
- [AdvancedDB_Project.ipynb](https://github.com/alexandrosst/AdvancedDB_Project/blob/main/AdvancedDB_Project.ipynb)

    Μπορεί να εκτελεστεί με τη βοήθεια της εντολής:
    ```bash
    jupyter-lab AdvancedDB_Project.ipynb --no-browser --ip=83.212.80.22
    ```
    Ανοίγουμε το link που εμφανίζεται στο τερματικό στο browser.

    <p align="justify">Σε περίπτωση που δουλεύουμε σε μηχάνημα με δυνατότητα να τρέξει το JupyterLab σε κάποιον browser μπορούμε να αγνοήσουμε τα flags.</p>