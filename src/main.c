#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define columnUsernameSize 32
#define columnEmailSize 255

typedef struct {
	char* buffer;
	size_t bufferLength;
	size_t inputLength;
} InputBuffer;

typedef struct {
    uint32_t id;
    char username[columnUsernameSize];
    char email[columnEmailSize];
} Row;

typedef enum {
    metaCommandSuccess,
    metaCommandUnrecognizedCommand
} MetaCommandResult;

typedef enum {
    prepareSuccess,
    prepareUnrecognizedStatement,
    prepareSyntaxError
} PrepareResult;

typedef enum {
    statementInsert,
    statementSelect
} StatementType;

typedef enum {
    executeSuccess,
    executeTableFull
} ExecuteResult;

typedef struct {
  StatementType type;
  Row rowToInsert;
} Statement;

// Compact representation of row
#define sizeOfAttribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

const uint32_t idSize = sizeOfAttribute(Row, id);
const uint32_t usernameSize = sizeOfAttribute(Row, username);
const uint32_t emailSize = sizeOfAttribute(Row, email);
const uint32_t idOffset = 0;
const uint32_t usernameOffset = idOffset + idSize;
const uint32_t emailOffset = usernameOffset + usernameSize;
const uint32_t rowSize = idSize + usernameSize + emailSize;

// Table structure
const uint32_t pageSize = 4096; // 4 kb
#define tableMaxPages 100
const uint32_t rowsPerPage = pageSize / rowSize;
const uint32_t tableMaxRows = rowsPerPage * tableMaxPages;

typedef struct {
    uint32_t numRows;
    void* pages[tableMaxPages];
} Table;

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

/**
 * @brief Creates and allocates memory for a new inputBuffer.
 * 
 * @return InputBuffer* 
 */
InputBuffer* newInputBuffer() {
	InputBuffer* inputBuffer = (InputBuffer*)malloc(sizeof(InputBuffer));
	inputBuffer->buffer = NULL;
	inputBuffer->bufferLength = 0;
	inputBuffer->inputLength = 0;

	return inputBuffer;
}

/**
 * @brief Prompts user for input.
 * 
 */
void printPrompt() {
	printf("db > ");
}

/**
 * @brief Prints the row.
 * 
 * @param row 
 */
void printRow(Row *row) {
    printf("%i %s %s\n", row->id, row->username, row->email);
}

/**
 * @brief Reads input from the user, calls getline, and trims whitespace(\n).
 * 
 * @param inputBuffer 
 */
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

/**
 * @brief Frees inputBuffer memory.
 * 
 * @param inputBuffer 
 */
void closeInputBuffer(InputBuffer* inputBuffer) {
	free (inputBuffer->buffer);
	free (inputBuffer);
}

/**
 * @brief getline from scratch. Reads a line of input from stdin (stream), stores input in *input, dynamically allocates (or resizes) memory, returns number of chars read.
 * 
 * @param lineptr Pointer to a buffer that stores the input string. If NULL, memory is allocated dynamically.
 * @param n Pointer to the size of the buffer.
 * @param stream Input stream to be read from.
 * @return size_t Number of chars read (excluding \n). -1 on failure.
 */
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

/**
 * @brief Handles meta commands.
 * 
 * @param inputBuffer 
 * @return MetaCommandResult 
 */
MetaCommandResult doMetaCommand(InputBuffer *inputBuffer) {
	if (strcmp(inputBuffer->buffer, ".exit") == 0) {
		exit(EXIT_SUCCESS);
    } else {
		return metaCommandUnrecognizedCommand;
	}
	
}

/**
 * @brief Creates a prepared statement.
 * 
 * @param inputBuffer 
 * @param statement 
 * @return PrepareResult 
 */
PrepareResult prepareStatement(InputBuffer *inputBuffer, Statement *statement) {
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        statement->type = statementInsert;
        int argsGiven = sscanf(inputBuffer->buffer, "insert %d %s %s", &(statement->rowToInsert.id), statement->rowToInsert.username, statement->rowToInsert.email);
        if (argsGiven < 3) {
            return prepareSyntaxError;
        }
        return prepareSuccess;
	}
    if (strcmp(inputBuffer->buffer, "select") == 0) {
        statement->type = statementSelect;
        return prepareSuccess;
    } else {
        return prepareUnrecognizedStatement;
    }
}

/**
 * @brief Serializes a row to fit into a contiguous memory block (page).
 * 
 * @param source Pointer to Row struct.
 * @param destination Pointer to page.
 */
void serializeRow(Row *source, void *destination) {
    memcpy(destination + idOffset, &(source->id), idSize);
    memcpy(destination + usernameOffset, source->username, usernameSize);
    memcpy(destination + emailOffset, source->email, emailSize);
}

/**
 * @brief Deserializes a contiguous memory block (page) to a row.
 * 
 * @param source Pointer to the page.
 * @param destination Pointer to Row struct.
 */
void deserializeRow(void *source, Row *destination) {
    memcpy(&(destination->id), source + idOffset, idSize);
    memcpy(destination->username, source + usernameOffset, usernameSize);
    memcpy(destination->email, source + emailOffset, emailSize);
}

/**
 * @brief Finds the location of the row in the table. If the page is unallocated, allocates memory for a new page.
 * 
 * @param table 
 * @param rowNum 
 * @return void* Pointer to row's location.
 */
void *rowSlot(Table *table, uint32_t rowNum) {
    uint32_t pageNumber = rowNum / rowsPerPage;
    void *page = table->pages[pageNumber];
    if (page == NULL) {
        page = table->pages[pageNumber] = malloc(pageSize);
    }
    uint32_t rowOffset = rowNum % rowsPerPage;
    uint32_t byteOffset = rowOffset * rowSize;
    return page + byteOffset;
}

/**
 * @brief Checks if the table is full, and, if not, inserts a row into the table.
 * 
 * @param table 
 * @param statement 
 * @return ExecuteResult 
 */
ExecuteResult executeInsert(Table *table, Statement *statement) {  
    if (table->numRows == tableMaxRows) {
        printf("Table is full.\n");
        return executeTableFull;
    }

    Row *row = &(statement->rowToInsert);
    serializeRow(row, rowSlot(table, table->numRows));
    table->numRows++;
    return executeSuccess;
}

/**
 * @brief Iterates over all rows and prints them.
 * 
 * @param table 
 * @param statement 
 * @return ExecuteResult 
 */
ExecuteResult executeSelect(Table *table, Statement *statement) {
    (void)statement; //suppress warning
    Row row;
    for (uint32_t i = 0; i < table->numRows; i++) {
        deserializeRow(rowSlot(table, i), &row);
        printRow(&row);
    }
    return executeSuccess;
}

/**
 * @brief Executes statement on a table.
 * 
 * @param statement 
 * @param table 
 */
ExecuteResult executeStatement(Statement *statement, Table* table) {
    switch (statement->type) {
        case (statementInsert):
            executeInsert(table, statement);
            return executeSuccess;
        case (statementSelect):
            executeSelect(table, statement);
            return executeSuccess;
    }
    return -1;
}

/**
 * @brief Allocates memory for a new table and sets all pages to NULL.
 * 
 * @return Table* 
 */
Table* newTable() {
    Table *table = (Table*)malloc(sizeof(Table));
    table->numRows = 0;

    for (uint32_t i = 0; i < tableMaxPages; i++) {
        table->pages[i] = NULL;
    }
    return table;
}

/**
 * @brief Frees memory for each page within a table and the table itself.
 * 
 * @param table 
 */
void freeTable(Table *table) {
    for (uint32_t i = 0; i < tableMaxPages; i++) {
        free(table->pages[i]);
    }
    free(table);
}

int main (int argc, char* argv[]) {
    Table* table = newTable();
    InputBuffer* inputBuffer = newInputBuffer();

	while (true) {
		printPrompt();
		readInput(inputBuffer);

        if (inputBuffer->buffer[0] == '.') {
			switch (doMetaCommand(inputBuffer)) {
				case (metaCommandSuccess):
					continue;
				case (metaCommandUnrecognizedCommand):
					printf("Command not recognized: '%s'\n", inputBuffer->buffer);
					continue;
			}
		}

		//Prepared statement
		Statement statement;
		switch (prepareStatement(inputBuffer, &statement)) {
		    case (prepareSuccess):
			    break;
		    case (prepareUnrecognizedStatement):
			    printf("Unrecognized keyword at '%s' \n", inputBuffer->buffer);
                continue;
            case (prepareSyntaxError):
                printf("Syntax error. \n");
                continue;
			continue;
		}

		switch(executeStatement(&statement, table)) {
            case (executeTableFull):
                printf("Error: Table is full.\n");
                break;
            case (executeSuccess):
                printf("Statement executed.\n");
                break;
        }
		
	}
}