package azkaban.project;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import org.yaml.snakeyaml.Yaml;


public class ProjectSpecLoader {

  public ProjectSpec load(File projectSpecFile) throws FileNotFoundException {
    return new Yaml().loadAs(new FileInputStream(projectSpecFile), ProjectSpec.class);
  }
}
