# kunskapskontroll-2
Python – Kunskapskontroll – DS2023

UpplandsRuninskrifterMain.py uses module to read Upplands runinskrifter from wikipedia and sucsessfully stores signum and text in the database. It can update the database if new pages or edits are available, and log file can be used to find articles without inscription text. Unfortunately I found no way to reliably detect translation to Swedish, because it is indistinguashible from the other Swedish text of the article in the API.

Testing can be done with pytest without disturbing the database, where I separately test reading and writing (due to some misbihave of mock function on my environment). Here I use pytest and pytest-mock to mimick the database without actually writing into it.
