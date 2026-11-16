#include <stdio.h>

#include "greet.h"

const char *greet(const char *name) {
    static char text[64];
    snprintf(text, sizeof text, "hola, %s", name);
    return text;
}
