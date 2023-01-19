#define _POSIX_C_SOURCE 200809L
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <signal.h>
#include <pthread.h>
#include <ctype.h>
#define BACKLOG 5

struct connection {
    struct sockaddr_storage addr;
    socklen_t addr_len;
    int fd;
};

struct node{
    char* value;
    char* key;
    struct node* next;
};

struct node* keys;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;


void *readkey(void *arg){
    char host[100], port[10], ch;
    struct connection *c = (struct connection *) arg;
    int error;
    error = getnameinfo((struct sockaddr *) &c->addr, c->addr_len, host, 100, port, 10, NI_NUMERICSERV);
    if (error != 0) {
        perror("error in getnameinfo\n");
        close(c->fd);
        return NULL;
    }
    printf("[%s:%s] connection\n", host, port);
    FILE *fin = fdopen(dup(c->fd), "r");//read file
    FILE *fout = fdopen(c->fd, "w");//write file
    char *buf = malloc(20);
    buf[0]='\0';
    while((ch = fgetc(fin)) != EOF){
        if(ch == '\n'){
        	pthread_mutex_lock(&mutex);
        	//printf("%s\n", buf);
            //got the message
            if(strcmp(buf, "GET") == 0){
            	char i[10];
            	i[0] = '\0';
            	ch = fgetc(fin);
                if(ch == '\n'){//multiple newlines after a message
                	write(c->fd, "ERR\nLEN\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }
                else if(!isdigit(ch)){
                    write(c->fd, "ERR\nBAD\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }
                strncat(i, &ch, 1);
                while((ch = fgetc(fin)) != '\n'){//read the full integer
                    if(!isdigit(ch)){
                        write(c->fd, "ERR\nBAD\n", 8);
                        fclose(fin);
                        fclose(fout);
                        free(buf);
                        close(c->fd);
                        free(c);
                        return NULL;
                    }
                    strncat(i, &ch, 1);
                }
             int counter = 0;
                buf[0] = '\0';//refresh the buffer
                while((ch = fgetc(fin)) != '\n'){//read the key
                 counter += 1;
                    strncat(buf, &ch, 1);
                }
                counter += 1;
                if(counter != atoi(i)){//incorrect key length
                 write(c->fd, "ERR\nLEN\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }    
                struct node* ptr = keys;//point to the corresponding key
                while(ptr != NULL){
                    if(strcmp(ptr->key, buf) == 0){
                        break;
                    }
                    ptr=ptr->next;
                }
                if(ptr == NULL){//the key is not set
                 write(c->fd, "KNF\n", 4);
                 //printf("reading new message\n");
                    buf[0] = '\0';
                    pthread_mutex_unlock(&mutex);
                    continue;
                }
                char num[10];
                //itoa(strlen(ptr->value)+1, num, 10);
                sprintf(num, "%ld", strlen(ptr->value)+1);
                write(c->fd, "OKG\n", 4);
                write(c->fd, num, strlen(num));
                write(c->fd, "\n", 1);
                write(c->fd, ptr->value, strlen(ptr->value));
                write(c->fd, "\n", 1);
            }
            else if(strcmp(buf,"SET") == 0){
            	char i[10];
            	i[0] = '\0';
                while((ch = fgetc(fin)) == '\n'){//in case there are several new lines after a message(error)
                	write(c->fd, "ERR\nLEN\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }
                if(!isdigit(ch)){
                    write(c->fd, "ERR\nBAD\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }
                strncat(i, &ch, 1);
                while((ch = fgetc(fin)) != '\n'){//read the full integer
                    if(!isdigit(ch)){
                        write(c->fd, "ERR\nBAD\n", 8);
                        fclose(fin);
                        fclose(fout);
                        free(buf);
                        close(c->fd);
                        free(c);
                        return NULL;
                    }
                    strncat(i, &ch, 1);
                }
             int counter = 0;
                buf[0] = '\0';//refresh the buffer
                while((ch = fgetc(fin)) != '\n'){//read the key
                    strncat(buf, &ch, 1);
                    counter+= 1;
                }
                counter+=1;
                char k[strlen(buf)+1];
                strcpy(k, buf);
                buf[0] = '\0';
                struct node* ptr = keys;//point to the corresponding key
                while(ptr != NULL){
                    if(strcmp(ptr->key, k) == 0){
                        break;
                    }
                    ptr = ptr->next;
                }
                while((ch = fgetc(fin)) != '\n'){//read the value
                    strncat(buf, &ch, 1);
                    counter += 1;
                }
                counter += 1;
                if(counter != atoi(i)){//incorrect key length
                 write(c->fd, "ERR\nLEN\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }
                if(ptr == NULL){//the key is not set
                    struct node* new = (struct node*)malloc(sizeof(struct node));
                    new->next = NULL;
                    new->key = malloc(strlen(k)+1);
                    new->value = malloc(strlen(buf)+1);
                    strcpy(new->key,k);
                    strcpy(new->value, buf);
                    if(keys == NULL){
                        keys = new;
                    }
                    else{
                    struct node* temp = keys;
                    while(temp->next != NULL){
                        temp = temp->next;
                    }
                    temp->next = new;
                    }
                }
                else{
                    ptr->value = realloc(ptr->value, sizeof(buf)+1);
                    strcpy(ptr->value, buf);
                }
                write(c->fd, "OKS\n", 4);
            }
            else if(strcmp(buf, "DEL") == 0){
            	char i[10];
            	i[0] = '\0';
                while((ch = fgetc(fin)) == '\n'){//in case there are several new lines after a message(error)
                	write(c->fd, "ERR\nLEN\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }
                if(!isdigit(ch)){
                    write(c->fd, "ERR\nBAD\n", 8);
                    fclose(fin);
                    fclose(fout);
                    free(buf);
                    close(c->fd);
                    free(c);
                    return NULL;
                }
                strncat(i, &ch, 1);
                while((ch = fgetc(fin)) != '\n'){//read the full integer
                    if(!isdigit(ch)){
                        write(c->fd, "ERR\nBAD\n", 8);
                        fclose(fin);
                        fclose(fout);
                        free(buf);
                        close(c->fd);
                        free(c);
                        return NULL;
                    }
                    strncat(i, &ch, 1);
                }
             int counter = 0;
                buf[0] = '\0';//refresh the buffer
                while((ch = fgetc(fin)) != '\n'){//read the key
                    strncat(buf, &ch, 1);
                    counter+=1;
                }
                counter+=1;
                struct node* ptr = keys;//point to the corresponding key
                while(ptr != NULL){
                    if(strcmp(ptr->key, buf) == 0){
                        break;
                    }
                }
                if(ptr == NULL || ptr->key == NULL){//the key is not set
                    write(c->fd, "KNF\n", 5);
                    //printf("reading new message\n");
                    buf[0] = '\0';
                    pthread_mutex_unlock(&mutex);
                    continue;
                }
                else{
                	char num[10];
                	//itoa(strlen(ptr->value)+1, num, 10);
                	sprintf(num, "%ld", strlen(ptr->value)+1);
                    write(c->fd, "OKD\n", 4);
                    write(c->fd, num, strlen(num));
                    write(c->fd, "\n", 1);
                    write(c->fd, ptr->value, strlen(ptr->value));
                    write(c->fd, "\n", 1);
                    free(ptr->key);
                    free(ptr->value);
                    ptr = keys;
                    if(ptr == keys){  //want to delete the first node
                     keys = ptr->next;
                     free(ptr);
                    }
                    else{
                     struct node* ptr2;
                     while(ptr->next != NULL){
                         if(strcmp(ptr->next->key, buf) == 0){
                             break;
                         }
                     }
                     ptr2 = ptr->next;
                     ptr->next = ptr->next->next;
                     free(ptr2);
                    }
                }
            }
            else{
                //error messages, write to fout the appropriate error message
                write(c->fd, "ERR\n", 4);
                if(strlen(buf) != 3){
                    write(c->fd, "LEN\n", 4);
                }
                else{
                    write(c->fd, "BAD\n", 4);
                }
                fclose(fin);
                fclose(fout);
                free(buf);
                close(c->fd);
                free(c);
                return NULL;
            }
            //printf("reading new message\n");
            buf[0] = '\0';
            pthread_mutex_unlock(&mutex);
        }
        else{
         strncat(buf, &ch, 1);
        }
    }
    printf("[%s:%s] got EOF\n", host, port);
    fclose(fin);
    fclose(fout);
    free(buf);
    close(c->fd);
    free(c);
    return NULL;
}

int main(int argc, char **argv){
    if(argc != 2){
        fprintf(stderr, "wrong argument\n");
        exit(EXIT_FAILURE);
    }
    keys = NULL;
    struct addrinfo hint, *info_list, *info;
    struct connection *con;
    int error, sfd;
    pthread_t tid;
    memset(&hint, 0, sizeof(struct addrinfo));
    hint.ai_family = AF_UNSPEC;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_flags = AI_PASSIVE;
    error = getaddrinfo(NULL, argv[1], &hint, &info_list);
    if (error != 0) {
        perror("error in getaddrinfo\n");
        exit(EXIT_FAILURE);
    }
    for (info = info_list; info != NULL; info = info->ai_next) {
        sfd = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
        if (sfd == -1) {
            continue;
        }
        if ((bind(sfd, info->ai_addr, info->ai_addrlen) == 0) && (listen(sfd, BACKLOG) == 0)) {
            break;
        }
        close(sfd);
    }
    if (info == NULL){
        perror("error in bind\n");
        exit(EXIT_FAILURE);
    }
    freeaddrinfo(info_list);

    // at this point sfd is bound and listening
    printf("Waiting for connection\n");
    
    for (;;){
        con = malloc(sizeof(struct connection));
        con->addr_len = sizeof(struct sockaddr_storage);
        con->fd = accept(sfd, (struct sockaddr *) &con->addr, &con->addr_len);
        if (con->fd == -1){
            perror("error in accept");
            continue;
        }
        error = pthread_create(&tid, NULL, readkey, con);


        if(error != 0){
            perror("error in creating pthread\n");
            close(con->fd);
            free(con);
            continue;
        }
        
        pthread_detach(tid);
            
    }
    
    struct node* ptr;
    while(keys != NULL){
        ptr = keys->next;
        free(keys->value);
        free(keys->key);
        free(keys);
        keys = ptr;
    }
    return 0;
}





