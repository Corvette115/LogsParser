#include <iostream>
#include <vector>
#include <map>
#include <ctime>
#include <regex>
#include <curl/curl.h>
#include "sqlite3.h"

/*"error":"",
 * "logs":[
 * {
 * "сreated_at":"2021-01-23T12:33:14"—датасозданиявформатеISO8601,
 * "first_name":"Матвей",-имяпользователя
 * "message":"I'mnotsure.Comeon!Mustgo!Notime!Butyou'repartofthisworld!you?!,",-фразапользователя
 * "second_name":"Иванов",-фамилияпользователя"
 * user_id":"726462"
 * }]*/
 

using namespace std;

size_t write_to_string(void *ptr, size_t size, size_t count, void *stream) {
  ((string*)stream)->append((char*)ptr, 0, size*count);
  return size*count;
}
static int callback(void *data, int argc, char **argv, char **azColName){
   int i;
   cout << data;
   
   for(i = 0; i < argc; i++) cout << azColName[i] << " : " << (argv[i] ? argv[i] : "NULL") << "\n";
   return 0;
}

class LogRec
{
	string fields[5] = {"created_at", "first_name", "message", "second_name", "user_id"};
	string info[5];
public:
	LogRec() {for (int i = 0; i < sizeof(fields)/sizeof(fields[0]); i++) info[i] = "";};
	LogRec(const LogRec& other) : info(other.info) {};
	LogRec& operator=(const LogRec& other) {
		if (this != &other) for (int i = 0; i < sizeof(fields)/sizeof(fields[0]); i++) info[i] = other.info[i];
		return *this;
		};
	LogRec& operator=(LogRec&& other) {
		for (int i = 0; i < sizeof(fields)/sizeof(fields[0]); i++) info[i] = other.info[i];
		return *this;
		};
	void init(string json);
	bool operator<(LogRec b) { return get("created_at") < b.get("created_at"); };
	string get(string field);
	string get_all() { return "\"" + info[0] + "\", \"" + info[1] + "\", \"" + info[2] + "\", \"" + info[3] + "\", " + info[4]; };
};

class LogParser
{
	sqlite3 *db = 0;
	LogRec *logs;
	size_t size = 0;
private:
	string getJson(string date);
	void json2Logs(string json);
	void sortLogs(LogRec ar[], size_t n);
	void toDB();
public:
	LogParser() { 
		char *zErrMsg = 0;
		sqlite3_open("test.db", &db); 
		sqlite3_exec(db, "create table if not exists logs(id integer primary key autoincrement, "\
			"created_at text, first_name text, message text, second_name text, user_id integer)", callback, 0, &zErrMsg);
		};
	~LogParser() { sqlite3_close(db); };
	void getLogs(string date);
};

void LogRec::init(string json)
{
	std::size_t pos, pos1;
	string base;
	for (int i = 0; i < sizeof(fields)/sizeof(fields[0]); i++)
	{
		pos = json.find("\"" + fields[i] + "\"");
		pos = json.find("\"", pos + 2 + fields[i].size());
		pos1 = json.find("\"", pos + 1);
		base = json.substr(pos + 1, pos1 - pos - 2);
		info[i] = base;
	}
}
string LogRec::get(string field)
{
	size_t a = 0;
	while (fields[a] != field && a < sizeof(fields)/sizeof(fields[0])) a++;
	return info[a];
}


void LogParser::json2Logs(string json)
{
	size_t pos = json.find("[");
	size_t pos1 = json.find("]");
	size = count(&json[pos + 1], &json[pos1], '{');
	
	logs = new LogRec[size];
	size_t count = 0;
	
	string base = json.substr(pos + 1, pos1-pos-1);
	if (base.size() > 0)
	{
		string::const_iterator searchStart(base.cbegin());
		pos = base.find("{");
		while (pos != string::npos)
		{
			pos1 = base.find("}", pos);
			logs[count].init(base.substr(pos, pos1-pos));
			pos = base.find("{", pos1);
			count++;
		}
	}
}
void LogParser::sortLogs(LogRec ar[], size_t n)
{
	if (n > 1)
	{
		size_t l_n = n/2; size_t r_n = n - l_n;
		
		sortLogs(&ar[0], l_n);
		sortLogs(&ar[l_n], r_n);
		
		size_t l_count = 0, r_count = l_n, count = 0;
		LogRec* tmp_ar(new LogRec[n]);
		while (l_count < l_n || r_count < n)
		{
			if (ar[l_count] < ar[r_count])
			{
				tmp_ar[count++] = move(ar[l_count]);
				l_count++;
			}
			else
			{
				tmp_ar[count++] = move(ar[r_count]);
				r_count++;
			}
			
			if (l_count == l_n)
            {
				copy(&ar[r_count], &ar[n], &tmp_ar[count]);
				break;
			}
			if (r_count == n)
			{
				copy(&ar[l_count], &ar[l_n], &tmp_ar[count]);
				break;
			}
		}
		copy(tmp_ar, &tmp_ar[n], ar);
		delete [] tmp_ar;
	}
}
void LogParser::toDB()
{
	for (int i = 0; i < size; i++)
	{
		string sql = "insert into logs (created_at, first_name, message, second_name, user_id) values "\
			"(" + logs[i].get_all() + ")";
		char *zErrMsg = 0;
		int rc = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
		if (rc != 0) 
		{
			cout << zErrMsg << "\n"; sqlite3_free(zErrMsg);
		}
	}
	cout << "Write to DB " << to_string(size) << " rows\n";
}

void LogParser::getLogs(string date)
{
	CURL *curl;
	CURLcode res;

	curl = curl_easy_init();
	if(curl) {
		string response, error;
		std::size_t pos, pos1;
		cout << "http://www.dsdev.tech/logs/" + date << '\n';
		curl_easy_setopt(curl, CURLOPT_URL, ("http://www.dsdev.tech/logs/" + date).c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_to_string);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

		res = curl_easy_perform(curl);
		if(res != CURLE_OK) 
		{
			cout << "Error while getting logs: " << curl_easy_strerror(res) << '\n';  
			return;
		}
		curl_easy_cleanup(curl);
		
		pos = response.find("error");
		pos = response.find('"', pos + 6);
		pos1 = response.find('"', pos + 1);
		error = response.substr(pos + 1, pos1 - pos - 1);
		if (error.empty())
		{
			json2Logs(response);
			sortLogs(logs, size);
			toDB();
		}
		else
		{
			cout << "Error: " << error << "\n";
			return;
		}
	}
	delete [] logs;
}

int main(int argc, char **argv)
{
	string date;
	if (argc == 1) date = "20210123";
	else date = argv[1];

	setlocale(LC_ALL, "Russian");
	LogParser lp = LogParser();
	lp.getLogs(date);
	return 0;
}
