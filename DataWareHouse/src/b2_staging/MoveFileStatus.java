package b2_staging;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class MoveFileStatus {
	static public void moveFileToError(String file) {
		File f = new File(file);
		String newPath = f.getParent() + File.separator + "Error" + File.separator + f.getName();
		try {
			Files.move(Paths.get(file), Paths.get(newPath), StandardCopyOption.REPLACE_EXISTING);
			System.out.println("Move file " + f.getName() + " to folder error \r\n");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Move file that bai");
		}
	}
	
	static public void moveFileToSuccess(String file) {
		File f = new File(file);
		String newPath = f.getParent() + File.separator + "Successfully" + File.separator + f.getName(); // path folder
		try {
			Files.move(Paths.get(file), Paths.get(newPath), StandardCopyOption.REPLACE_EXISTING);
			System.out.println("Move file " + f.getName() + "  to folder successfully \r\n");
		} catch (IOException e) {
			System.out.println("Move file that bai");
		}
	}
}
