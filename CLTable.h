#pragma once
#include <pthread.h>
#include <map>

class CLTable
{
public:
    int SaveIndex(int attibute);
    static CLTable *GetInstance();
    static int WriteRowMsg(const int64_t *pstrMsg);
    static int ReadRowMsg();


    static int64_t **SearchFromTable(int attribute, int64_t low, int64_t high);

private:
    static void OnProcessExit();
    int64_t *m_pReader;
    static pthread_mutex_t *InitializeMutex();
    std::map<int64_t, int64_t> RowsHash;

private:
    CLTable(const CLTable &);
    CLTable &operator=(const CLTable &);

    int WriteRow(const int64_t *pstrMsg);
    int ReadRow();
    int Flush();
    int ReadIndex(int attribute);
    int64_t **Search(int attribute, int64_t low, int64_t high);
    int64_t **SearchWithIndex(int attribute, int64_t low, int64_t high);
    CLTable();
    ~CLTable();

private:
    int m_Fd;
    int64_t m_nRows;
    int64_t m_nReaderRows;
    pthread_mutex_t *m_pMutexForWritingTable;
    // int m_nReaders;
    static CLTable *m_pTable;
    static pthread_mutex_t *m_pMutexForCreatator;

private:
    int64_t *m_pTableBuffer;
    int64_t m_nUsedRowsForBuffer;

private:
    bool m_bFlagForProcessExit;
};
