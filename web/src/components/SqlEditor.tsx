import React, { useMemo } from "react";
import CodeMirror, {
  Prec,
  keymap,
  type Extension,
} from "@uiw/react-codemirror";
import { sql, MySQL, PostgreSQL } from "@codemirror/lang-sql";

export interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  dialect?: "mysql" | "postgres";
  placeholder?: string;
  height?: string;
  minHeight?: string;
  maxHeight?: string;
  readOnly?: boolean;
  disabled?: boolean;
  onExecute?: () => void;
  autoFocus?: boolean;
  className?: string;
  style?: React.CSSProperties;
}

export const SqlEditor: React.FC<SqlEditorProps> = ({
  value,
  onChange,
  dialect = "mysql",
  placeholder,
  height = "160px",
  minHeight,
  maxHeight,
  readOnly = false,
  disabled = false,
  onExecute,
  autoFocus = false,
  className,
  style,
}) => {
  const extensions = useMemo(() => {
    const exts: Extension[] = [
      sql({
        dialect: dialect === "postgres" ? PostgreSQL : MySQL,
      }),
    ];

    if (onExecute) {
      exts.push(
        Prec.highest(
          keymap.of([
            {
              key: "Mod-Enter",
              run: () => {
                onExecute();
                return true;
              },
            },
          ])
        )
      );
    }

    return exts;
  }, [dialect, onExecute]);

  return (
    <div
      className={`ds-sql-editor ${className || ""}`}
      style={{
        ...(height ? { height } : {}),
        ...(minHeight ? { minHeight } : {}),
        ...(maxHeight ? { maxHeight } : {}),
        ...style,
      }}
    >
      <CodeMirror
        value={value}
        height={height ? "100%" : "auto"}
        minHeight={minHeight}
        maxHeight={maxHeight}
        extensions={extensions}
        onChange={onChange}
        placeholder={placeholder}
        readOnly={readOnly || disabled}
        autoFocus={autoFocus}
        basicSetup={{
          lineNumbers: true,
          highlightActiveLineGutter: true,
          highlightSpecialChars: true,
          history: true,
          foldGutter: true,
          drawSelection: true,
          dropCursor: true,
          allowMultipleSelections: true,
          indentOnInput: true,
          syntaxHighlighting: true,
          bracketMatching: true,
          closeBrackets: true,
          autocompletion: true,
          rectangularSelection: true,
          crosshairCursor: true,
          highlightActiveLine: true,
          highlightSelectionMatches: true,
          closeBracketsKeymap: true,
          defaultKeymap: true,
          searchKeymap: true,
          historyKeymap: true,
          foldKeymap: true,
          completionKeymap: true,
          lintKeymap: true,
        }}
      />
    </div>
  );
};

export default SqlEditor;
