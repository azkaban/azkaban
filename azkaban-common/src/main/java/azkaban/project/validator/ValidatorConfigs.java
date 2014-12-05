package azkaban.project.validator;

public class ValidatorConfigs {

  private ValidatorConfigs() {} // Prevents instantiation

  /** Key for the config param specifying the directory containing validator JAR files **/
  public static final String VALIDATOR_PLUGIN_DIR = "project.validators.dir";

  /** Default validator directory **/
  public static final String DEFAULT_VALIDATOR_DIR = "validators";

  /** Key for the config param specifying the location of validator xml configuration file, no default value **/
  public static final String XML_FILE_PARAM = "project.validators.xml.file";

  /** Key for the config param indicating whether the user choose to turn on the auto-fix feature **/
  public static final String CUSTOM_AUTO_FIX_FLAG_PARAM = "project.validators.fix.flag";

  /** Default custom auto fix flag. Turn auto-fix feature on by default. **/
  public static final Boolean DEFAULT_CUSTOM_AUTO_FIX_FLAG = true;

  /** Key for the config param indicating whether to show auto-fix related UI to the user **/
  public static final String VALIDATOR_AUTO_FIX_PROMPT_FLAG_PARAM = "project.validators.fix.prompt";

  /** Do not show auto-fix related UI by default **/
  public static final Boolean DEFAULT_VALIDATOR_AUTO_FIX_PROMPT_FLAG = false;

  /** Key for the config param specifying the label to be displayed with auto-fix UI **/
  public static final String VALIDATOR_AUTO_FIX_PROMPT_LABEL_PARAM = "project.validators.fix.label";

  /** Key for the config param specifying the link address with detailed information about auto-fix **/
  public static final String VALIDATOR_AUTO_FIX_PROMPT_LINK_PARAM = "project.validators.fix.link";

  /** Key for the confi param indicating path to the project archive file **/
  public static final String PROJECT_ARCHIVE_FILE_PATH = "project.archive.file.path";
}
