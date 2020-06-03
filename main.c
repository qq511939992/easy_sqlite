#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include<stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
//定义数据输入格式
ssize_t getline(char** line, size_t* n, FILE* fp)
{
    char* buf = *line;
    ssize_t c, i = 0;//i来记录字符串长度，c来存储字符
    if (buf == NULL || *n == 0)
    {
        *line = malloc(10);
        buf = *line;
        *n = 10;
    }
    //buf为或n为0时动态为期分配空间
    while ((c = fgetc(fp)) != '\n')
    {
        if (c == EOF)
            return -1;
        if (i < *n - 2)//留2个空间给\n和\0
        {
            *(buf + i++) = c;
        }
        else
        {
            *n = *n + 10;
            buf = realloc(buf, *n);//空间不足时，重新进行分配
            *(buf + i++) = c;
        }
    }
    *(buf + i++) = '\n';
    *(buf + i) = '\0';
    return i;

}



//定义了输入结构
typedef struct {
    char* buffer;//输入字符
    size_t buffer_length;//占用内存空间长度
    ssize_t input_length;//输入字符长度，结尾为‘/0’
} InputBuffer;

//枚举元数据命令结果
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;

//枚举准备执行的指令是否可行
typedef enum {
    PREPARE_SUCCESS,PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,PREPARE_STRING_TOO_LONG,PREPARE_NEGATIVE_ID
}PrepareReslut;

//枚举指令类型
typedef enum {
    STATEMENT_INSERT,STATEMENT_SELECT
}StatementType;

//枚举指令执行状态码
typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_DUPLICATE_KEY//键重复
}ExecuteResult;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
//定义insert数据每一行的格式
typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
}Row;

//指令结构包含指令类型和指令数据
typedef struct {
    StatementType type;
    Row row_to_insert;
}Statement;

//节点类型
typedef  enum{
    NODE_INTERNAL,
    NODE_LEAF
}NodeType;

//定义一个宏指令，实现读取Struct->Attribute类型的内存空间大小
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

//定义字段所占内存大小
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);

//定义字段起始位置
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET=ID_OFFSET+ID_SIZE;
const uint32_t EMAIL_OFFSET=USERNAME_OFFSET+USERNAME_SIZE;
const uint32_t ROW_SIZE=ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;
//定义分页属性
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100


//一般节点头设计（节点单位为页）
const uint32_t NODE_TYPE_SIZE=sizeof(uint8_t);//节点类型所占空间
const uint32_t NODE_TYPE_OFFSET=0;//节点类型偏移量
const uint32_t IS_ROOT_SIZE=sizeof(uint8_t);//根节点标志所占空间
const uint32_t IS_ROOT_OFFSET=NODE_TYPE_SIZE;//根节点标志偏移量
const uint32_t PARENT_POINTER_SIZE=sizeof(uint32_t);//指向父节点指针所占空间
const uint32_t PARENT_POINTER_OFFSET=IS_ROOT_OFFSET+IS_ROOT_SIZE;//指向父节点指针偏移量
const uint8_t COMMON_NODE_HEADER_SIZE=NODE_TYPE_SIZE+IS_ROOT_SIZE+PARENT_POINTER_SIZE;//节点头所占空间

//叶节点头设计
const uint32_t LEAF_NODE_NUM_CELLS_SIZE=sizeof(uint32_t);//叶节点内胞元数量所占空间
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET=COMMON_NODE_HEADER_SIZE;//叶节点内胞元数量偏移量(在一般基础上)
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE=sizeof(uint32_t);//兄弟节点所在页码指针所占空间
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET=LEAF_NODE_NUM_CELLS_OFFSET+LEAF_NODE_NUM_CELLS_SIZE;//兄弟节点所在页码指针偏移量
const uint32_t LEAF_NODE_HEADER_SIZE=
        COMMON_NODE_HEADER_SIZE+
        LEAF_NODE_NUM_CELLS_SIZE+
        LEAF_NODE_NEXT_LEAF_SIZE;//叶节点头所占空间

//叶节点体设计
const uint32_t LEAF_NODE_KEY_SIZE=sizeof(uint32_t);//胞元关键词所占空间
const uint32_t LEAF_NODE_KEY_OFFSET=0;//胞元关键词偏移量
const uint32_t LEAF_NODE_VALUE_SIZE=ROW_SIZE;//胞元值所占空间(等于数据行所占空间行)
const uint32_t LEAF_NODE_VALUE_OFFSET=LEAF_NODE_KEY_OFFSET+LEAF_NODE_KEY_SIZE;//胞元值偏移量
const uint32_t LEAF_NODE_CELL_SIZE=LEAF_NODE_KEY_SIZE+LEAF_NODE_VALUE_SIZE;//胞元大小（关键词+值）
const uint32_t LEAF_NODE_SPACE_FOR_CELLS=PAGE_SIZE-COMMON_NODE_HEADER_SIZE;//每一页留给所有胞元的空间
const uint32_t LEAF_NODE_MAX_CELLS=
        LEAF_NODE_SPACE_FOR_CELLS/LEAF_NODE_CELL_SIZE;//最大胞元数量

//内节点头设计
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE=sizeof(uint32_t);//内节点关键词数量所占内存
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET=COMMON_NODE_HEADER_SIZE;//内节点关键词数量偏移地址
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE=sizeof(uint32_t);//内节点右孩子地址所占内存
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET=
        INTERNAL_NODE_NUM_KEYS_OFFSET+INTERNAL_NODE_NUM_KEYS_SIZE;//内节点右孩子地址的偏移地址
const uint32_t INTERNAL_NODE_HEADER_SIZE=COMMON_NODE_HEADER_SIZE+
        INTERNAL_NODE_NUM_KEYS_SIZE+
        INTERNAL_NODE_RIGHT_CHILD_SIZE;//内节点头大小

//内节点体设计
const uint32_t INTERNAL_NODE_CHILD_SIZE=sizeof(uint32_t);//孩子指针
const uint32_t INTERNAL_NODE_KEY_SIZE=sizeof(uint32_t);//关键词
const uint32_t INTERNAL_NODE_CELL_SIZE=
        INTERNAL_NODE_CHILD_SIZE+INTERNAL_NODE_KEY_SIZE;//一个元胞由孩子指针和关键词组成
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

//定义页面缓存结构
typedef struct{
    int file_descriptor;//文件描述
    uint32_t file_length;//文件长度
    uint32_t num_pages;//页总页数
    void* pages[TABLE_MAX_PAGES];//定义页
}Pager;

//定义数据表结构：行数，页
typedef struct {
    uint32_t root_page_num;//root_page所在页数
    Pager* pager;//包含页面缓存结构
}Table;

typedef  struct {
    Table* table;
    uint32_t page_num;//光标所指向的页编号
    uint32_t cell_num;//光标所指向的胞元编号
    bool end_of_table;//是否表尾的标识
}Cursor;

void print_row(Row* row) {
    printf("(%d,%s,%s)\n", row->id, row->username, row->email);
}

//返回节点胞元数量的OFFSET
uint32_t *leaf_node_num_cells(void* node){
    return node+LEAF_NODE_NUM_CELLS_OFFSET;
}

//返回编号为cell_num的胞元的OFFSET
void* leaf_node_cell(void *node,uint32_t cell_num){
    return node+LEAF_NODE_HEADER_SIZE+cell_num*LEAF_NODE_CELL_SIZE;
}

//返回编号为cell_num的胞元的关键词的OFFSET
uint32_t *leaf_node_key(void* node,uint32_t cell_num){
    return leaf_node_cell(node,cell_num);
}

//返回编号为cell_num的胞元的值的OFFSET
void* leaf_node_value(void* node,uint32_t cell_num){
    return leaf_node_cell(node,cell_num)+LEAF_NODE_KEY_SIZE;
}

//返回节点类型
NodeType get_node_type(void *node){
    uint8_t value=*((uint8_t*)(node+NODE_TYPE_OFFSET));
    return (NodeType)value;
}

//设置节点类型
void set_node_type(void *node,NodeType type){
    uint8_t value=type;
    *((uint8_t*)(node+NODE_TYPE_OFFSET))=value;
}

//返回是否为根节点
bool is_node_root(void* node){
    uint8_t value=*((uint8_t*)(node+IS_ROOT_OFFSET));
    return (bool)value;
}

//设置节点是否为根节点
void set_node_root(void *node,bool is_root){
    uint8_t value=is_root;
    *((uint8_t*)(node+IS_ROOT_OFFSET))=value;
}

//返回下一个兄弟节点的页码
uint32_t *leaf_node_next_leaf(void *node){
    return node+LEAF_NODE_NEXT_LEAF_OFFSET;
}

//初始化一个叶节点
void initialize_leaf_node(void *node){
    set_node_type(node,NODE_LEAF);
    set_node_root(node,false);
    *leaf_node_next_leaf(node)=0;
    *leaf_node_num_cells(node)=0;
}

//返回内节点关键词数量的地址
uint32_t *internal_node_num_keys(void *node){
    return node+INTERNAL_NODE_NUM_KEYS_OFFSET;
}

//初始化根节点
void initialize_internal_node(void *node){
    set_node_type(node,NODE_INTERNAL);
    set_node_root(node,false);
    *internal_node_num_keys(node)=0;
}

//获得父节点所在页码编号
uint32_t *node_parent(void* node){return node+PARENT_POINTER_OFFSET;}

//序列化：将数据写入内存
void serialize_row(Row* source, void* destination) {
    memcpy(destination+ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

//反序列化：将内存中的数据提取出来
void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

//为当前page分配内存并反对对应页内容
void* get_page(Pager* pager,uint32_t page_num){
    //如果输入页数大于数据表最大页数，则打印错误并退出
    if(page_num>TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds.%d>%d\n",
                page_num,TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    //为空白页分配内存
    if (pager->pages[page_num]==NULL){
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages=pager->file_length/PAGE_SIZE;
        if(pager->file_length%PAGE_SIZE){
            num_pages+=1;
        }
        //在输入页数小于数据库文件中页数时，将读取指针定位到文件的page_num * PAGE_SIZE个字节处，并将文件中一页内容赋值给page所在地址内存
        if(page_num<=num_pages){
            lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
            ssize_t bytes_read=read(pager->file_descriptor,page,PAGE_SIZE);
            if(bytes_read==-1){
                printf("Error reading file:%d\n",errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num]=page;
        if(page_num>=pager->num_pages){
            pager->num_pages=page_num+1;
        }
    }
    return pager->pages[page_num];
}



//返回当前数据行的值
void* cursor_value(Cursor* cursor){
    uint32_t page_num=cursor->page_num;
    void* page=get_page(cursor->table->pager,page_num);//获得pager->page[page_num]所指向的内容首地址
    return leaf_node_value(page,cursor->cell_num);//返回光标指向胞元的值
}

//光标下移到下一个胞元
void cursor_advance(Cursor* cursor){
    uint32_t page_num=cursor->page_num;//获取光标指向页编号
    void* node=get_page(cursor->table->pager,page_num);//获取光标指向页
    cursor->cell_num+=1;//光标指向下一个胞元
    //如果光标指向节点尾，则设置表尾标识为true
    if(cursor->cell_num>=(*leaf_node_num_cells(node))){
        uint32_t next_page_num=*leaf_node_next_leaf(node);
        //如果没有兄弟节点，则读取结束
        //如果有兄弟节点，则相应赋值光标
        if(next_page_num==0){
            cursor->end_of_table=true;
        } else{
            cursor->page_num=next_page_num;
            cursor->cell_num=0;
        }
    }
}

//将pager->pages[page_num]中所有数据写入pager->file_descriptor
void pager_flush(Pager* pager,uint32_t page_num){
    if(pager->pages[page_num]==NULL){
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
    if(offset==-1){
        printf("Error seeking:%d\n",errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written=write(pager->file_descriptor,pager->pages[page_num],PAGE_SIZE);
    if(bytes_written==-1){
        printf("Error writing:%d\n",errno);
        exit(EXIT_FAILURE);
    }
}

//退出数据库时，将文件保存并将内存空间释放
void db_close(Table* table){
    Pager * pager =table->pager;//提取分页器
    //保存到文件
    for (uint32_t i=0;i<pager->num_pages;i++){
        if(pager->pages[i]==NULL){
            continue;
        }
        pager_flush(pager,i);//将pager->pages[i]中PAGE_SIZE大小的内容存入pager->file_descriptor指向的文件
        free(pager->pages[i]);//释放pager->pages[i]的内存
        pager->pages[i]=NULL;
    }
    //关闭数据库文件
    int result =close(pager->file_descriptor);
    if(result==-1){
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    //多此一举？之前不是释放过了吗
    for (uint32_t i=0;i<TABLE_MAX_PAGES;i++){
        void* page=pager->pages[i];
        if(page){
            free(page);
            pager->pages[i]=NULL;
        }
    }
    free(pager);
    free(table);
}

//初始化页面缓存器
Pager* pager_open(const char* filename){
    int fd=open(filename,O_RDWR|O_CREAT,S_IWUSR|S_IRUSR);//以读写方式打开文件
    //打开失败则答应错误并退出
    if (fd==-1){
        printf("Unable to open file.\n");
        exit(EXIT_FAILURE);
    }
    //计算文件长度
    off_t file_length = lseek(fd,0,SEEK_END);
    //初始化分页器
    Pager* pager = malloc(sizeof(Pager));//初始化页面缓存器
    pager->file_descriptor=fd;//将文件标记符赋值给页面缓存器
    pager->file_length=file_length;//将文件长度赋值给页面缓存器
    pager->num_pages=(file_length/PAGE_SIZE);//计算文件总页数赋值给num_pages
    //如果加载出的文件不是整数个页面长度，则中断读取
    if(file_length%PAGE_SIZE!=0){
        printf("Db file is not a whole number of pages.Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i=0;i<TABLE_MAX_PAGES;i++){
        pager->pages[i]=NULL;
    }
    return pager;
}

//定义一个新的数据表
Table* db_open(const char* filename){
    Pager* pager = pager_open(filename);//将文件读取到页面缓存器
    Table *table=malloc(sizeof(Table));//初始化数据表
    table->pager = pager;//初始化页面缓存器，将文件中值导入
    table->root_page_num=0;//设置表的0编号页为根节点
    //如果是新文件，则初始化使第0页为根节点
    if(pager->num_pages==0){
        void *root_node=get_page(pager,0);
        initialize_leaf_node(root_node);
        set_node_root(root_node,true);
    }
    return table;
}



//返回内节点右孩子指针的地址
uint32_t *internal_node_right_child(void* node){
    return node+INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

//返回内节点中元胞地址
uint32_t *internal_node_cell(void* node,uint32_t cell_num){
    return node+INTERNAL_NODE_HEADER_SIZE+cell_num*INTERNAL_NODE_CELL_SIZE;
}

//返回内节点中孩子节点所在页码
uint32_t* internal_node_child(void* node,uint32_t child_num){
    uint32_t num_keys=*internal_node_num_keys(node);//获取内节点关键词数量
    //如果孩子编号大于关键词数，返回错误
    if(child_num>num_keys){
        printf("Tried to access child_num %d>num_keys %d\n",child_num,num_keys);
        exit(EXIT_FAILURE);
    }else if(child_num==num_keys){
        return internal_node_right_child(node);//孩子编号和关键词数相等，返回右孩子节点地址
    }else{
        return internal_node_cell(node,child_num);//孩子编号小于关键词数，返回对应元胞地址
    }
}

//返回内节点关键词地址
uint32_t *internal_node_key(void *node,uint32_t key_num){
    return (void*)internal_node_cell(node,key_num)+INTERNAL_NODE_CHILD_SIZE;
}

//分配新页面数
uint32_t get_unused_page_num(Pager* pager){return pager->num_pages;}

//返回节点关键词最大值
uint32_t get_node_max_key(void *node){
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *internal_node_key(node,*internal_node_num_keys(node)-1);
        case NODE_LEAF:
            return *leaf_node_key(node,*leaf_node_num_cells(node)-1);

    }
}

//创建新的根节点，并指向左右孩子
void create_new_root(Table* table,uint32_t right_child_page_num){
    //初始化左右孩子
    void* root=get_page(table->pager,table->root_page_num);
    void *right_child=get_page(table->pager,right_child_page_num);
    uint32_t left_child_page_num=get_unused_page_num(table->pager);
    void* left_child=get_page(table->pager,left_child_page_num);
    memcpy(left_child,root,PAGE_SIZE);//把根节点复制到左孩子
    set_node_root(left_child, false);
    //将根页面初始化为具有两个子节点的新内部节点
    initialize_internal_node(root);
    set_node_root(root,true);
    *internal_node_num_keys(root)=1;//关键词数量为1
    *internal_node_child(root,0)=left_child_page_num;//编号为0的孩子节点为left_child
    uint32_t left_child_max_key=get_node_max_key(left_child);//获取左孩子最大关键词
    *internal_node_key(root,0)=left_child_max_key;//设置root编号为0的关键词为左孩子最大关键词
    *internal_node_right_child(root)=right_child_page_num;//设置右孩子指针（以页码为标志）
    *node_parent(left_child)=table->root_page_num;//左孩子的父节点设置为根节点
    *node_parent(right_child)=table->root_page_num;//右孩子的父节点设置为根节点
}

//分配分割后节点元胞数
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT=(LEAF_NODE_MAX_CELLS+1)/2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT=(LEAF_NODE_MAX_CELLS+1)-LEAF_NODE_RIGHT_SPLIT_COUNT;



//该函数用于生成一个输入所需初始化空间
InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}


//每次循环先在屏幕显示‘db > ’
void print_prompt() { printf("db >"); }

//读取输入到input_buffer
void read_input(InputBuffer* input_buffer) {
//    使用getline读取内容到buffer，并把所字符数传给buffer_length
    ssize_t bytes_read =getline(&(input_buffer->buffer),
            &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // 删掉换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

//释放input_buffer内存
void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

//将读取到的输入划分为不同的模块，每个模块存入相应的位置，并返回PrepareReslut
PrepareReslut prepare_insert(InputBuffer* input_buffer,Statement* statement){
    statement->type=STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer," ");
    char* id_string=strtok(NULL," ");
    char* username = strtok(NULL," ");
    char* email = strtok(NULL," ");
    char* extra = strtok(NULL," ");

    if (id_string==NULL||username==NULL||email==NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    if (extra!=NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    int id=atoi(id_string);
    if (id<0){
        return PREPARE_NEGATIVE_ID;
    }
    if(strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email)>COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert.id=id;
    strcpy(statement->row_to_insert.username,username);
    strcpy(statement->row_to_insert.email,email);
    return PREPARE_SUCCESS;
}
//判断指令类型并返回指令状态码（是否有效）
PrepareReslut prepare_statement(InputBuffer* input_buffer,Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer,statement);
    }
    if (strcmp(input_buffer->buffer, "select")==0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

//计算插入元素的对应元胞编号
Cursor *leaf_node_find(Table* table,uint32_t page_num,uint32_t key){
    void* node=get_page(table->pager,page_num);//读取对应页数编号的页内容到node
    uint32_t num_cells=*leaf_node_num_cells(node);//读取节点中的元胞数量
    Cursor *cursor=malloc(sizeof(Cursor));//初始化一个光标
    cursor->table=table;
    cursor->page_num=page_num;

    uint32_t min_index=0;
    uint32_t one_past_max_index=num_cells;//之前的最大元胞数
    //计算出的位置特征：插入key所对应的元胞编号
    while(one_past_max_index!=min_index){
        uint32_t index=(min_index+one_past_max_index)/2;
        uint32_t key_at_index=*leaf_node_key(node,index);
        if(key<=key_at_index){
            one_past_max_index=index;
        }else{
            min_index=index+1;
        }
    }
    cursor->cell_num=min_index;
    return cursor;
}

//查找内节点中小于等于key所对应的元胞编号
uint32_t internal_node_find_child(void *node,uint32_t key) {

    uint32_t num_keys = *internal_node_num_keys(node);//获取内节点关键词数量
    uint32_t min_index = 0;
    uint32_t max_index = num_keys;
    //二分搜索
    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    return min_index;
}

//二分搜索键对应的胞元所在位置（cursor）
Cursor *internal_node_find(Table* table,uint32_t page_num,uint32_t key){
    void* node=get_page(table->pager,page_num);
    uint32_t child_index=internal_node_find_child(node,key);
    uint32_t child_num=*internal_node_child(node,child_index);
    void* child =get_page(table->pager,child_num);//读取对应孩子节点
    //如果孩子节点为叶子结点返回对应key所在胞元编号，如果是内节点，则递归搜索直到找到叶子结点
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table,child_num,key);
        case NODE_INTERNAL:
            return internal_node_find(table,child_num,key);
    }

}

//更新内节点关键词
void update_internal_node_key(void* node,uint32_t old_key,uint32_t new_key){
    uint32_t old_child_index=internal_node_find_child(node,old_key);//获取内节点中旧key位置
    *internal_node_key(node,old_child_index)=new_key;//为旧key赋新值
}

//添加一个孩子指针/关键词对到父节点
void internal_node_insert(Table* table,uint32_t parent_page_num,uint32_t child_page_num){
    void* parent=get_page(table->pager,parent_page_num);//获得父节点地址
    void* child=get_page(table->pager,child_page_num);//获得孩子节点地址
    uint32_t child_max_key=get_node_max_key(child);//获得孩子节点中最大的关键词
    uint32_t index=internal_node_find_child(parent,child_max_key);//找到内节点中关键词小于等于key的元胞编号
    uint32_t original_num_keys=*internal_node_num_keys(parent);//获得父节点关键词数量
    *internal_node_num_keys(parent)=original_num_keys+1;
    //如果多余设定的关键词数量，我们需要开始拆分这个内节点
    if(original_num_keys>=INTERNAL_NODE_MAX_CELLS){
        printf("Need to implement splitting internal node\n");
        exit(EXIT_FAILURE);
    }
    uint32_t right_child_page_num=*internal_node_right_child(parent);//获得父节点右孩子所在页码
    void* right_child=get_page(table->pager,right_child_page_num);
    if(child_max_key>get_node_max_key(right_child)){
        //新增孩子节点若关键词比原右孩子关键词大，则替换右孩子
        *internal_node_child(parent,original_num_keys)=right_child_page_num;//父节点最后一个孩子替换为原右孩子(原本为空)
        *internal_node_key(parent,original_num_keys)=get_node_max_key(right_child);
        *internal_node_right_child(parent)=child_page_num;
    }else{
        //将应插入key位置之后的所有元胞向后移动一个单元
        for(uint32_t i=original_num_keys;i>index;i--){
            void* destination = internal_node_cell(parent,i);
            void* source=internal_node_cell(parent,i-1);
            memcpy(destination,source,INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent,index)=child_page_num;//插入子孩子指针
        *internal_node_key(parent,index)=child_max_key;//插入孩子节点关键词
    }
}

//创建新的叶子结点并分割当前节点元素
void leaf_node_split_and_insert(Cursor* cursor,uint32_t key,Row* value){
    void* old_node=get_page(cursor->table->pager,cursor->page_num);//获取当前节点
    uint32_t old_max=get_node_max_key(old_node);//获取旧节点的最大关键词
    uint32_t new_page_num=get_unused_page_num(cursor->table->pager);//从页面缓存器中获取未使用页面编号
    void *new_node=get_page(cursor->table->pager,new_page_num);//根据未使用的页面编号创建新节点
    initialize_leaf_node(new_node);//初始化新节点为叶子结点
    *node_parent(new_node)=*node_parent(old_node);//使new_node和old_node指向同一个父节点
    *leaf_node_next_leaf(new_node)=*leaf_node_next_leaf(old_node);//新节点的兄弟为旧节点的兄弟
    *leaf_node_next_leaf(old_node)=new_page_num;//旧结点的兄弟为新节点
    //将旧节点的元素一分为二，序号小的放旧结点，需要大的放在新节点
    for(int32_t i=LEAF_NODE_MAX_CELLS;i>=0;i--){
        void* destination_node;
        if(i>=LEAF_NODE_LEFT_SPLIT_COUNT){
            destination_node=new_node;
        }else{
            destination_node=old_node;
        }
        uint32_t index_within_node=i%LEAF_NODE_LEFT_SPLIT_COUNT;//重新定义元胞编号
        void *destination=leaf_node_cell(destination_node,index_within_node);//获取元胞应存入的地址
        if(i==cursor->cell_num){
            serialize_row(value,leaf_node_value(destination_node,index_within_node));
            *leaf_node_key(destination_node,index_within_node)=key;
        }else if(i>cursor->cell_num){
            memcpy(destination,leaf_node_cell(old_node,i-1),LEAF_NODE_CELL_SIZE);//新节点编号从0开始
        }else{
            memcpy(destination,leaf_node_cell(old_node,i),LEAF_NODE_CELL_SIZE);
        }
    }
    //更新新旧节点的元胞数
    *(leaf_node_num_cells(old_node))=LEAF_NODE_LEFT_SPLIT_COUNT;//旧结点中大于LEAF_NODE_LEFT_SPLIT_COUNT编号的元素未删除，只更新元胞数
    *(leaf_node_num_cells(new_node))=LEAF_NODE_RIGHT_SPLIT_COUNT;
    //需要更新节点的父节点
    if(is_node_root(old_node)){
        return create_new_root(cursor->table,new_page_num);//若旧结点为根节点，则创建新节点充当父节点
    } else{
        uint32_t parent_page_num=*node_parent(old_node);//提取旧节点父节点页码编号
        uint32_t new_max=get_node_max_key(old_node);//提取旧节点最大关键词
        void* parent=get_page(cursor->table->pager,parent_page_num);
        update_internal_node_key(parent,old_max,new_max);//更新内节点关键词
        internal_node_insert(cursor->table,parent_page_num,new_page_num);//插入内节点
        return;
    }
}

//插入新的叶子结点胞元
void leaf_node_insert(Cursor* cursor,uint32_t key,Row* value){
    void* node=get_page(cursor->table->pager,cursor->page_num);//获得当前光标指向的节点
    uint32_t num_cells=*leaf_node_num_cells(node);//获得当前节点的胞元数量
    //如果胞元总数大于叶节点最大胞元容载量，则创建新的叶子结点并分割当前节点元素
    if(num_cells>=LEAF_NODE_MAX_CELLS){
        leaf_node_split_and_insert(cursor,key,value);
        return;
    }
    //如果光标指向的编号胞元小于节点当前胞元总数，cell_num开始的胞元全部向后移一格
    if(cursor->cell_num<num_cells){
        for (uint32_t i=num_cells;i>cursor->cell_num;i--){
            memcpy(leaf_node_cell(node,i),leaf_node_cell(node,i-1),LEAF_NODE_CELL_SIZE);
        }
    }
    *(leaf_node_num_cells(node))+=1;//节点胞元数量+1
    *(leaf_node_key(node,cursor->cell_num))=key;//将关键词保存到光标指定胞元编号的关键词
    serialize_row(value,leaf_node_value(node,cursor->cell_num));//将value中的元素放到光标指定的胞元处
}


//返回胞元插入位置
Cursor *table_find(Table* table,uint32_t key){
    uint32_t root_page_num=table->root_page_num;//初始化根节点编号
    void* root_node=get_page(table->pager,root_page_num);//初始化根节点
    if(get_node_type(root_node)==NODE_LEAF){
        return leaf_node_find(table,root_page_num,key);//返回元胞插入位置
    }else{
        return internal_node_find(table,root_page_num,key);//返回胞元插入位置
    }
}
//初始化光标（表头）
Cursor* table_start(Table* table){
    Cursor *cursor=table_find(table,0);//查找key为0的光标(即使不存在也会返回开头的光标)

    void *node = get_page(table->pager,cursor->page_num);
    uint32_t num_cells =*leaf_node_num_cells(node);
    cursor->end_of_table=(num_cells==0);
    return cursor;
}

//执行insert指令将输入输出到内存并返回执行状态码
ExecuteResult execute_insert(Statement* statement, Table* table) {
    Row* row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert=row_to_insert->id;
    Cursor *cursor=table_find(table,key_to_insert);//返回胞元插入位置
    void* node=get_page(table->pager,cursor->page_num);//读节点
    uint32_t num_cells=(*leaf_node_num_cells(node));
    if (cursor->cell_num<num_cells){
        uint32_t key_at_index=*leaf_node_key(node,cursor->cell_num);
        //防止键重复
        if (key_at_index==key_to_insert){
            return EXECUTE_DUPLICATE_KEY;
        }
    }
    leaf_node_insert(cursor,row_to_insert->id,row_to_insert);//将数据输出到内存
    free(cursor);
    return EXECUTE_SUCCESS;
}

//打印系统常量
void print_constants(){
    printf("ROW_SIZE:%d\n",ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE:%d\n",COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE:%d\n",LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE:%d\n",LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS:%d\n",LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS:%d\n",LEAF_NODE_MAX_CELLS);
}

//树的可视化
void indent(uint32_t level){
    for (uint32_t i=0;i<level;i++){
        printf(" ");
    }
}
void print_tree(Pager* pager,uint32_t page_num,uint32_t indentation_level){
    void *node=get_page(pager,page_num);
    uint32_t num_keys,child;
    switch (get_node_type(node)) {
        case (NODE_LEAF):
            num_keys=*leaf_node_num_cells(node);//获取关键词数量
            indent(indentation_level);
            printf("- leaf (size %d)\n",num_keys);//打印关键词数量
            //打印每一个关键词
            for(uint32_t i=0;i<num_keys;i++){
                indent(indentation_level+1);
                printf("- %d\n",*leaf_node_key(node,i));
            }
            break;
        case (NODE_INTERNAL):
            num_keys=*internal_node_num_keys(node);//获取关键词数量
            indent(indentation_level);
            printf("- internal (size %d)\n",num_keys);//打印关键词数量
            //先打印左节点，再打印关键词
            for(uint32_t i=0;i<num_keys;i++){
                child=*internal_node_child(node,i);//孩子节点指向页数
                print_tree(pager,child,indentation_level+1);
                indent(indentation_level+1);
                printf("- key %d\n",*internal_node_key(node,i));
            }
            //最后打印右节点
            child=*internal_node_right_child(node);
            print_tree(pager,child,indentation_level+1);
            break;
    }
}

//执行元命令并返回状态码
MetaCommandResult do_meta_command(InputBuffer* input_buffer,Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }else if(strcmp(input_buffer->buffer,".constants")==0){
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }
    else if(strcmp(input_buffer->buffer,".btree")==0){
        printf("Tree:\n");
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    }
    else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}



//执行反序列化将数据全部读出显示屏幕
ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    Cursor* cursor=table_start(table);
    while(!(cursor->end_of_table)){
        deserialize_row(cursor_value(cursor),&row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

//执行指令
ExecuteResult execute_statement(Statement* statement,Table* table) {
    switch(statement->type){
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case(STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main(int argc, char* argv[]) {
    if (argc<2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }
    char* filename=argv[1];
    Table* table=db_open(filename);
    InputBuffer* input_buffer = new_input_buffer();//创建一个初始化输入空间
    //循环REPL
    while (true) {
        print_prompt();//打印‘db > ’
        read_input(input_buffer);//读取字符到input_buffer（地址空间）
        //判断是正常指令还是元指令
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer,table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }
        //定义一条指令包含指令类型和指令内容
        Statement statement;
        //转到合适的具体指令，如insert将数据装进input_to_insert，并返回状态码
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
                continue;
            case(PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case(PREPARE_NEGATIVE_ID):
                printf("ID must be postivite.\n");
                continue;
        }
        switch (execute_statement(&statement, table))
        {
            case(EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error:Table full.\n");
                break;
            case(EXECUTE_DUPLICATE_KEY):
                printf("Error: Duplicate key.\n");
                break;
        }
    }
}