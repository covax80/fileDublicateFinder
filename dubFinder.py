#!python
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
from pathlib import Path
import hashlib
import xxhash
from time import time
from multiprocessing.dummy import Pool as ThreadPool
import asyncio
from prettytable import PrettyTable

CONN 			= None
DUBLICATE_FILENAMES 	= []
DUBLICATE_FILES_ID 	= []
THREADS			= 18

def init_db():
	global CONN
	CONN 	= sqlite3.connect("database.sqlite3")
	cursor  = CONN.cursor()			
	if os.path.exists("database.sqlite3"):
		cursor.execute("""CREATE TABLE IF NOT EXISTS FILES([id] INTEGER PRIMARY KEY, [filename] TEXT, [folder] TEXT, [filesize] INTEGER, [filehash] TEXT);""")		
	return cursor


def make_all_files_array( folders ):
	"""insert into DATABASE all files and their folders"""
	global CONN
	cursor = init_db()	
	query  = None
	for folder in folders:
		for folder_name, _, fileList in os.walk(folder):
			for filename in fileList:
				#filename 	= os.path.join(folder, filename)			
				query 		= """ INSERT INTO files (filename, folder) VALUES (?, ?)"""
				cursor.execute(query,(filename,folder_name))
	CONN.commit()

def delete_all_nondublicated():
	global CONN, DUBLICATE_FILENAME
	cursor  = CONN.cursor()				
	#print("""DELETE FROM files WHERE filename NOT IN ({});""".format(",".join(["\'"+ x + "\'" for x in DUBLICATE_FILENAMES])))
	cursor.execute("""DELETE FROM files WHERE filename NOT IN ({});""".format(",".join(["\'"+ x + "\'" for x in DUBLICATE_FILENAMES])))
	cursor.execute("""SELECT count(*) FROM files;""");print(cursor.fetchall()[0][0])
	CONN.commit()
	return


def delete_all_nondublicated_id():
	global CONN, DUBLICATE_FILES_ID
	cursor = CONN.cursor()
	#print("""DELETE FROM files WHERE id NOT IN ({:s})""".format(  ",".join(  [str(x) for x in DUBLICATE_FILES_ID]  ) ) )
	cursor.execute("""DELETE FROM files WHERE id NOT IN ({:s});""".format(  ",".join(  [str(x) for x in DUBLICATE_FILES_ID]  ) ) )
	CONN.commit()
	cursor.execute("""SELECT count(*) FROM files;""");print(cursor.fetchall()[0][0])
	return
        

def find_dublicate_filenames():
	"""delete all records from DATABASE exclude dublicates"""
	global CONN, DUBLICATE_FILENAMES, DUBLICATE_FILES_ID
	cursor  = CONN.cursor()				
	cursor.execute("""SELECT filename, COUNT(id) as dublicates FROM files GROUP BY filename HAVING dublicates > 1 ORDER BY dublicates DESC""")		
	for filename,dublicates in cursor.fetchall(): 
		#print("|\t{}\t\t|\t{}\t|".format(filename,dublicates) )
		DUBLICATE_FILENAMES.append(filename)		

	cursor.execute("""SELECT count(*) FROM files;""");print("TOTAL RECORDS: ",cursor.fetchall()[0][0])		

	for filename in DUBLICATE_FILENAMES:
		cursor.execute("""SELECT id FROM files WHERE filename = '{}' """.format(filename))
		for file_id in cursor.fetchall():
			DUBLICATE_FILES_ID.append(file_id[0])
	print("RECORDS AFTER REMOVE NON-DUBLICATED FILES: ", end=" ")
	delete_all_nondublicated_id()
	#delete_all_nondublicated()
	return
		


def filter_by_size():
	"""delete all records from DATABASE exclude dublicated files with same size"""
	global CONN, DUBLICATE_FILENAMES
	filesize = 0
	cursor  = CONN.cursor()				
	for filename in DUBLICATE_FILENAMES:
		#print(filename)
		query = """SELECT id, filename, folder FROM files WHERE filename = ?"""	
		cursor.execute(query, [filename] )
		for file_id, filename, folder in cursor.fetchall():
			try:
				filesize = Path( os.path.join(folder, filename) ).stat().st_size		
			except FileNotFoundError:
				filesize = 0
			query = ''' UPDATE files SET filesize = ? WHERE id = ?'''
			cursor.execute(query, (filesize,file_id) )	
	query = """SELECT filename, filesize, COUNT(id) as dublicates FROM files GROUP BY filename,filesize HAVING dublicates > 1 ORDER BY dublicates DESC """
	cursor.execute(query) 
	DUBLICATE_FILENAMES = []
	for filename,filesize,dublicates in cursor.fetchall(): 
		DUBLICATE_FILENAMES.append(filename)
		#print("|\t{}\t\t|\t{}\t\t|\t{}\t|".format(filename,filesize,dublicates) )
		cursor.execute("""DELETE FROM files WHERE filename  = ? AND filesize <> ? """, (filename, filesize))
	CONN.commit()
	#cursor.execute("""SELECT count(*) FROM files;""");
	print("RECORDS AFTER SIZE FILTER: ", end=" ")	
	delete_all_nondublicated()
	return

def hashfile(abs_path_file, blocksize = 65536):
#def hashfile(abs_path_file, blocksize = 4096):
	try:
		f = open(abs_path_file, 'rb')
	except FileNotFoundError:				
		return '00000000'
	#hasher = hashlib.md5()
	hasher = xxhash.xxh32()
	buffer = f.read(blocksize)
	while len(buffer) > 0:
		hasher.update(buffer)
		buffer = f.read(blocksize)
	f.close()
	return hasher.hexdigest()
	#return hasher.intdigest()


async def cur_execute(cursor, query, data):		
	cursor.execute(query, data) 
	#loop = asyncio.get_event_loop()
	#return await loop.run_in_executor( None, lambda: cursor.execute(query, data) )

def filter_by_hash_threads():
	global CONN, DUBLICATE_FILENAMES, THREADS
	pool = ThreadPool(THREADS)		
	#res  = pool.map(check_model, list_for_check_model)

	ids = {}

	cursor  = CONN.cursor()				
	for filename in DUBLICATE_FILENAMES:
		#print(filename)
		query = '''SELECT id, filename, folder FROM files WHERE filename = '{}' '''.format(filename)
		cursor.execute( query )
		for file_id, filename, folder in cursor.fetchall():
			full_filename = os.path.join(folder, filename)
			ids[file_id] = full_filename
			
	res  = pool.map( hashfile, ids.values() )
	#print(res)
	idx = 0
	loops = []
	for file_id in ids.keys():
		query = ''' UPDATE files SET filehash = ? WHERE id = ?'''
		cursor.execute(query, (res[idx],file_id) )	
		#loops = asyncio.get_event_loop()
		#loops.run_until_complete( cur_execute(cursor, query, (res[idx],file_id) ) )		
		idx += 1
	CONN.commit()

	query = """SELECT filename, filehash, COUNT(id) as dublicates FROM files GROUP BY filename,filehash HAVING dublicates > 1 ORDER BY dublicates DESC"""
	cursor.execute( query )

	DUBLICATE_FILENAMES = []
	for filename,filehash,dublicates in cursor.fetchall():
		DUBLICATE_FILENAMES.append( filename )
		#print("|\t{}\t\t|\t{}\t\t|\t{}\t|".format(filename,filehash,dublicates) )		
		cursor.execute("""DELETE FROM files WHERE (filename  = ? AND filehash <> ?) """, (filename, filehash))
		#print("DELETE ",cursor.fetchall())
	CONN.commit()
	print("RECORDS AFTER HASH FILTER: ", end=" ")	
	delete_all_nondublicated()
	return

        
def filter_by_hash():
	"""delete all records from DATABASE exclude dublicated files with same MD5 hash"""
	global CONN, DUBLICATE_FILENAMES
	#print(DUBLICATE_FILENAMES)
	filehash = 0
	cursor  = CONN.cursor()				
	for filename in DUBLICATE_FILENAMES:
		#print(filename)
		query = '''SELECT id, filename, folder FROM files WHERE filename = '{}' '''.format(filename)
		cursor.execute( query )
		for file_id, filename, folder in cursor.fetchall():
			filehash = hashfile( os.path.join(folder, filename) )		
			query = ''' UPDATE files SET filehash = ? WHERE id = ?'''
			cursor.execute(query, (filehash,file_id) )			
	query = """SELECT filename, filehash, COUNT(id) as dublicates FROM files GROUP BY filename,filehash HAVING dublicates > 1 ORDER BY dublicates DESC"""
	cursor.execute( query )

	DUBLICATE_FILENAMES = []
	for filename,filehash,dublicates in cursor.fetchall():
		DUBLICATE_FILENAMES.append( filename )
		print("|\t{}\t\t|\t{}\t\t|\t{}\t|".format(filename,filehash,dublicates) )		
		cursor.execute("""DELETE FROM files WHERE (filename  = ? AND filehash <> ?) """, (filename, filehash))
		#print("DELETE ",cursor.fetchall())
	CONN.commit()
	
	print("RECORDS AFTER HASH FILTER: ", end=" ")	

	delete_all_nondublicated()

	return

def show_dublicates():
	global CONN, DUBLICATE_FILENAMES
	cursor  = CONN.cursor()	
	#cursor.execute("""SELECT count(*) FROM files;""");print("RECORDS: ",  cursor.fetchall()[0][0])
	#cursor.execute("""SELECT * FROM files;""");print(cursor.fetchall())
	query = """SELECT filename, folder, filehash, filesize FROM files ORDER BY filename,folder ASC"""	
	cursor.execute(query)
	full_filename = None
	t = PrettyTable(['File', 'HASH', 'SIZE'])
	t.align['File'] = "l"
	t.align['HASH'] = "r"
	t.align['SIZE'] = "r"
	for filename, folder, filehash, filesize in cursor.fetchall():
		full_filename = (os.path.join( folder, filename ))
		t.add_row([full_filename, filehash, "{:.1f} Kb".format( filesize/1024 ) ] )
		"""
		try:
			print( full_filename, end = "\t")				
			t.add_row([full_filename, filehash, "{:.1f} Kb".format( filesize/1024 ) ] )
		except UnicodeEncodeError as err:		
			print( str(err) + ": " + full_filename.encode('utf8','replace'), end = "\t")				
			t.add_row([str(err) + ": " + full_filename.encode('utf8','replace'), filehash, "{:.1f} Kb".format( filesize/1024 ) ] )
		"""
		"""try:
			print( "{}\t\t{}\t{:.1f} Kb\n".format( full_filename, filehash, filesize/1024 ) )
		except UnicodeEncodeError:
		        open('log3','ab').write( full_filename.encode('utf8','replace') )"""
	try:
		print(t)
	except UnicodeEncodeError as err:
		print("Unicode error: {0}".format(err))

	open('output.html','wb').write( (t.get_html_string()).encode('utf8','replace') )	
	CONN.close()
	return
	                 

def main( folders="." ): 
	t1 = time()
	make_all_files_array( folders )

	t2 = time()
	find_dublicate_filenames()
	
	t3 = time()
	filter_by_size()
        
	t4 = time()
	#filter_by_hash()
	filter_by_hash_threads()

	t5 = time()
	show_dublicates()
	print("\n\n_____PROFILER_____:\nADD TO BASE = {:.2f} s. \nFIND DUBL= {:.2f} s.\nFILTER SIZE = {:.2f} s.\nFILTER HASH = {:.2f}".format(t2-t1,t3-t2,t4-t3,t5-t4))
		

if __name__ == '__main__':
	if len(sys.argv) > 1:
		if os.path.exists("database.sqlite3"):
			os.remove('database.sqlite3')
		folders = []
		tmp_folders = sys.argv[1:]
		for folder in tmp_folders:          
			if os.path.exists( folder ):
				folders.append( folder )
			else:
				print('%s is not a valid path, please verify' % folder)
				sys.exit()
		main( folders )

	else:
        	print('Usage: python dubFinder.py folder1 folder2 folder3 ...')









