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
                , WORKFLOW_NAME='BI_CSAB912B',WORKFLOW_ID='c1b9c378ae3d4c3a91ed89862407a795', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSAB912B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 8, 11, 26, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c1b9c378ae3d4c3a91ed89862407a795')

    csab912bJob_vol = []
    csab912bJob_volMnt = []
    csab912bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csab912bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csab912bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csab912bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csab912bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csab912bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3f4660fbc9624222ab3af7704c167574',
        'volumes': csab912bJob_vol,
        'volume_mounts': csab912bJob_volMnt,
        'env_from':csab912bJob_env,
        'task_id':'csab912bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.4',
        'arguments':["--job.name=csab912bJob", "inFlag=A", "inDate=20240401", "testBatchRequestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c1b9c378ae3d4c3a91ed89862407a795')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csab912bJob,
        Complete
    ]) 

    # authCheck >> csab912bJob >> Complete
    workflow








