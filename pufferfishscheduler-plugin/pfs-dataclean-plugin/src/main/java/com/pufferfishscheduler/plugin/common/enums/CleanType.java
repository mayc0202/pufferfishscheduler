package com.pufferfishscheduler.plugin.common.enums;

/**
 * 清洗方式
 */
public enum CleanType {
    STRING_TRUNCATION("StringTruncation"),
    STRING_REPLACEMENT("StringReplacement"),
    STRING_PADDING("StringPadding"),
    DELETE_BLANK_CHARACTERS("DeleteBlankCharacters"),
    CONVERT_STRING_TO_NUMBER("ConvertStringToNumber"),
    CONVERT_NUMBER_TO_STRING("ConvertNumberToString"),
    LETTER_CAPITALIZATION_CONVERSION("LetterCapitalizationConversion"),
    CONVERT_STRING_TO_DATE("ConvertStringToDate"),
    CONVERT_DATE_TO_STRING("ConvertDateToString"),
    CONVERT_DATE_TO_TIMESTAMP("ConvertDateToTimeStamp"),
    ROUNDING("Rounding"),
    DESENSITIZATION("Desensitization"),
    CONVERT_BLANK_TO_NULL("ConvertBlankToNull"),
    REPLACE_NULL_TO_SPECIFY_VALUE("ReplaceNullToSpecifyValue"),
    CONVERT_ARABIC_NUMERALS_TO_CHINESE("ConvertArabicNumeralsToChinese"),
    JAVA_CUSTOM_RULE("JavaCustomRule"),
    VALUE_MAPPING("ValueMapping");

    private String description;

    CleanType(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }
}
