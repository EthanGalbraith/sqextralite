#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char* buffer;
    size_t bufferLength;
    size_t inputLength;
} InputBuffer;

/**
 * @brief prints "db" at the beginning of each line prompting a user command
 * 
 */
void printPrompt();

/**
 * @brief reads the user input
 * 
 * @param inputBuffer 
 */
void readInput(InputBuffer *inputBuffer);

/**
 * @brief frees memory from buffer and inputBuffer
 * 
 * @param inputBuffer 
 */
void closeInputBuffer(InputBuffer *inputBuffer);

/**
 * @brief creates a new InputBuffer
 * 
 * @return InputBuffer* 
 */
InputBuffer *newInputBuffer();

/**
 * @brief Reads a line from a file stream and dynamically resizes
 * 
 * @param lineptr A pointer to a dynamically allocated buffer
 * @param n A pointer to the size of the buffer
 * @param stream The input file stream
 * @return size_t The number of bytes read and -1 if fails
 */
size_t getline(char **lineptr, size_t *n, FILE *stream);

InputBuffer* newInputBuffer() {
    InputBuffer* inputBuffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->bufferLength = 0;
    inputBuffer->inputLength = 0;

    return inputBuffer;
}

void printPrompt() {
    printf("db > ");
}

void readInput (InputBuffer* inputBuffer) {
    size_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->bufferLength), stdin);

    if (bytesRead <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    //Trim whitespace
    if (bytesRead > 0 && inputBuffer->buffer[bytesRead - 1] == '\n') {
        inputBuffer->buffer[bytesRead - 1] = '\0';
        inputBuffer->inputLength = bytesRead - 1;
    } else {
        inputBuffer->inputLength = bytesRead;
    }    
}

void closeInputBuffer(InputBuffer* inputBuffer) {
    free (inputBuffer->buffer);
    free (inputBuffer);
}

size_t getline(char **lineptr, size_t *n, FILE *stream) {

    //No buffer exists, so allocate memory for one
    if (*lineptr == NULL) {
        *n = 128;
        *lineptr = (char*)malloc(*n);

        //If malloc fails
        if (*lineptr == NULL) {
            return -1;
        }
    }

    int ch;
    size_t length = 0;
    while (((ch = fgetc(stream)) != EOF && ch != '\n')) {

        //Double the size if the buffer is full
        if (length +1 >= *n) {
            *n *=2;
            *lineptr = (char*)realloc(*lineptr, *n);
            if (*lineptr == NULL) {
                return -1;
            }
        }
        (*lineptr)[length++] = ch;
    }
    (*lineptr)[length] = '\0';
    return length;
}


int main (int argc, char* argv[]) {
    InputBuffer* inputBuffer = newInputBuffer();

    while (true) {
        printPrompt();
        readInput(inputBuffer);
        
        if (strcmp(inputBuffer->buffer, ".exit") == 0) {
            closeInputBuffer(inputBuffer);
            exit(EXIT_SUCCESS);
        } else {
            printf("Command not recognized '%s'\n", inputBuffer->buffer);
        }
    }
}