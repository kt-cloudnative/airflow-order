from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG560B',WORKFLOW_ID='bdde70eb119f48048f8bee59d316f5cd', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG560B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bdde70eb119f48048f8bee59d316f5cd')

    csng560bJob_vol = []
    csng560bJob_volMnt = []
    csng560bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng560bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng560bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng560bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng560bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng560bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '00535cddcbe54f40b32936fcf01b199d',
        'volumes': csng560bJob_vol,
        'volume_mounts': csng560bJob_volMnt,
        'env_from':csng560bJob_env,
        'task_id':'csng560bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng560bJob","requestDate="+str(datetime.now()),"intbutDate=20250103","regOfcCd=252023","regerEmpNo=932894755","regerEmpName=홍길동"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bdde70eb119f48048f8bee59d316f5cd')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng560bJob,
        Complete
    ]) 

    # authCheck >> csng560bJob >> Complete
    workflow








