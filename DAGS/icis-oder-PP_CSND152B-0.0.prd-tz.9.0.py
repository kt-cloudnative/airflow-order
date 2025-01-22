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
                , WORKFLOW_NAME='PP_CSND152B',WORKFLOW_ID='a171a1b85ba7409cb5ab384acf3bf79a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND152B-0.0.prd-tz.9.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a171a1b85ba7409cb5ab384acf3bf79a')

    csnd152bJob_vol = []
    csnd152bJob_volMnt = []
    csnd152bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnd152bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnd152bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd152bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnd152bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd152bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '59c0ebefda784c43b9a76692bc2d103b',
        'volumes': csnd152bJob_vol,
        'volume_mounts': csnd152bJob_volMnt,
        'env_from':csnd152bJob_env,
        'task_id':'csnd152bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csnd152bJob","endTranDate=20241129","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a171a1b85ba7409cb5ab384acf3bf79a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd152bJob,
        Complete
    ]) 

    # authCheck >> csnd152bJob >> Complete
    workflow








