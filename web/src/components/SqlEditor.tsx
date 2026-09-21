import React, { useMemo, useState } from "react";
import CodeMirror, {
  EditorView,
  Prec,
  keymap,
  type Extension,
} from "@uiw/react-codemirror";
import { FullscreenExitOutlined, FullscreenOutlined } from "@ant-design/icons";
import { Button } from "antd";
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
  /** 显示工具条:自动换行开关与全屏切换 */
  toolbar?: boolean;
  className?: string;
  style?: React.CSSProperties;
}

export const SqlEditor: React.FC<SqlEditorProps> = ({
  value,
  onChange,
  dialect = "mysql",
  placeholder,
  height,
  minHeight,
  maxHeight,
  readOnly = false,
  disabled = false,
  onExecute,
  autoFocus = false,
  toolbar = false,
  className,
  style,
}) => {
  const [lineWrap, setLineWrap] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);

  const extensions = useMemo(() => {
    const exts: Extension[] = [
      sql({
        dialect: dialect === "postgres" ? PostgreSQL : MySQL,
      }),
    ];

    if (lineWrap) {
      exts.push(EditorView.lineWrapping);
    }

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
  }, [dialect, onExecute, lineWrap]);

  // height 显式给定(或全屏)时容器高度确定,.cm-theme 需进入 flex 链才能正确滚动;
  // 未传 height 的用法(如 minHeight/maxHeight 自适应长高)保持原有 auto 布局
  const effectiveHeight = height ?? "160px";
  const fixed = !!height || fullscreen;

  return (
    <div
      className={`ds-sql-editor ${fixed ? "ds-sql-editor-fixed" : ""} ${
        fullscreen ? "ds-editor-fullscreen" : ""
      } ${className || ""}`}
      style={{
        ...(effectiveHeight ? { height: effectiveHeight } : {}),
        ...(minHeight ? { minHeight } : {}),
        ...(maxHeight ? { maxHeight } : {}),
        ...style,
      }}
    >
      {toolbar && (
        <div className="ds-sql-editor-toolbar">
          <Button
            size="small"
            type={lineWrap ? "primary" : "text"}
            onClick={() => setLineWrap((v) => !v)}
          >
            自动换行
          </Button>
          <Button
            size="small"
            type="text"
            icon={fullscreen ? <FullscreenExitOutlined /> : <FullscreenOutlined />}
            onClick={() => setFullscreen((v) => !v)}
          >
            {fullscreen ? "退出全屏" : "全屏"}
          </Button>
        </div>
      )}
      <div className="ds-sql-editor-body">
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
    </div>
  );
};

export default SqlEditor;
